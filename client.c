#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <inttypes.h>
#include <endian.h>
#include <byteswap.h>
#include <getopt.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include "sock.h"
#define MAX_POLL_CQ_TIMEOUT 2000
#define MAX_KEY 8
#define MAX_VALUE 1024
#define HT_SIZE 256

#if __BYTE_ORDER == __LITTLE_ENDIAN
static inline uint64_t htonll(uint64_t x) { return bswap_64(x); }
static inline uint64_t ntohll(uint64_t x) { return bswap_64(x); }
#elif __BYTE_ORDER == __BIG_ENDIAN
static inline uint64_t htonll(uint64_t x) { return x; }
static inline uint64_t ntohll(uint64_t x) { return x; }
#else
#error __BYTE_ORDER is neither __LITTLE_ENDIAN nor __BIG_ENDIAN
#endif

/* structure of test parameters */
struct config_t {
	const char			*dev_name;	/* IB device name */
	char				*server_name;	/* daemon host name */
	u_int32_t			tcp_port;	/* daemon TCP port */
	int				ib_port;	/* local IB port to work with */
};

/* structure to exchange data which is needed to connect the QPs */
struct cm_con_data_t {
	uint64_t 			data_addr;		/* data address */
	uint64_t 			index_addr;		/* md address */

	uint32_t 			data_key;		/* data key */
	uint32_t 			index_key;			/* md key */
	
	uint32_t 			qp_num;		/* QP number */
	uint16_t 			lid;		/* LID of the IB port */
} __attribute__ ((packed));

/* structure of needed test resources */
struct resources {
	struct ibv_device_attr		device_attr;	/* Device attributes */
	struct ibv_port_attr		port_attr;	/* IB port attributes */
	struct cm_con_data_t		remote_props;	/* values to connect to remote side */
	struct ibv_context		*ib_ctx;	/* device handle */
	struct ibv_pd			*pd;		/* PD handle */
	struct ibv_cq			*cq;		/* CQ handle */
	struct ibv_qp			*qp;		/* QP handle */

	struct ibv_mr			*data_mr;	/* data mr handle */
	struct ibv_mr			*index_mr;		/* md mr handle */

	char				*data_buf;		/* data buffer pointer */
	char 				*index_buf;			// md buf

	int				sock;		/* TCP socket file descriptor */
};

struct hashtable{
	int exist[HT_SIZE];
	int location[HT_SIZE];
	char key[HT_SIZE][MAX_KEY];
};
const int total_size = 1024 * 1024;
int loc = 0;
struct config_t config = {
	"mlx5_1",			/* dev_name */
	"192.168.141.9",	/* server_name */
	2333,				/* tcp_port */
	1				/* ib_port */
};

static int poll_completion(
	struct resources *res)
{
	struct ibv_wc wc;
	unsigned long start_time_msec, cur_time_msec;
	struct timeval cur_time;
	int rc;


	/* poll the completion for a while before giving up of doing it .. */
	gettimeofday(&cur_time, NULL);
	start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);

	do {
		rc = ibv_poll_cq(res->cq, 1, &wc);
		if (rc < 0) {
			fprintf(stderr, "poll CQ failed\n");
			return 1;
		}
		gettimeofday(&cur_time, NULL);
		cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
	} while ((rc == 0) && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));

	/* if the CQ is empty */
	if (rc == 0) {
		fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
		return 1;
	}

	fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);

	/* check the completion status (here we don't care about the completion opcode */
	if (wc.status != IBV_WC_SUCCESS) {
		fprintf(stderr, "got bad completion with status: 0x%x, vendor syndrome: 0x%x\n", 
			wc.status, wc.vendor_err);
		return 1;
	}

	return 0;
}


//write data or index
static int post_write(struct resources *res, int data)
{
	struct ibv_send_wr sr;
	struct ibv_sge sge;
	struct ibv_send_wr *bad_wr;
	int rc;


	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	if(data == 1){ // data
		sge.addr = (uintptr_t)res->data_buf;
		sge.length = total_size;
		sge.lkey = res->data_mr->lkey;
	}
	else{
		sge.addr = (uintptr_t)res->index_buf;
		sge.length = sizeof(struct hashtable);
		sge.lkey = res->index_mr->lkey;
	}

	/* prepare the SR */
	memset(&sr, 0, sizeof(sr));

	sr.next       = NULL;
	sr.wr_id      = 0;
	sr.sg_list    = &sge;
	sr.num_sge    = 1;
	sr.opcode     = IBV_WR_RDMA_WRITE;
	sr.send_flags = IBV_SEND_SIGNALED;

	if(data == 1){
		sr.wr.rdma.remote_addr = res->remote_props.data_addr;
		sr.wr.rdma.rkey        = res->remote_props.data_key;
	}
	else{
		sr.wr.rdma.remote_addr = res->remote_props.index_addr;
		sr.wr.rdma.rkey        = res->remote_props.index_key;
	}

	/* there is a Receive Request in the responder side, so we won't get any into RNR flow */
	rc = ibv_post_send(res->qp, &sr, &bad_wr);
	if (rc) {
		fprintf(stderr, "failed to post SR\n");
		return 1;
	}
	fprintf(stdout, "Write Request was posted\n");

	return 0;
}

//read location
static int post_read(struct resources *res, int data)
{
	struct ibv_send_wr sr;
	struct ibv_sge sge;
	struct ibv_send_wr *bad_wr;
	int rc;


	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));

	if(data == 1){
		sge.addr = (uintptr_t)res->data_buf;
		sge.length = total_size;
		sge.lkey = res->data_mr->lkey;
	}
	else{
		sge.addr = (uintptr_t)res->index_buf;
		sge.length = sizeof(struct hashtable);
		sge.lkey = res->index_mr->lkey;	
	}

	/* prepare the SR */
	memset(&sr, 0, sizeof(sr));

	sr.next       = NULL;
	sr.wr_id      = 0;
	sr.sg_list    = &sge;
	sr.num_sge    = 1;
	sr.opcode     = IBV_WR_RDMA_READ;
	sr.send_flags = IBV_SEND_SIGNALED;

	if(data == 1){
		sr.wr.rdma.remote_addr = res->remote_props.data_addr;
		sr.wr.rdma.rkey        = res->remote_props.data_key;
	}
	else{
		sr.wr.rdma.remote_addr = res->remote_props.index_addr;
		sr.wr.rdma.rkey        = res->remote_props.index_key;
	}

	/* there is a Receive Request in the responder side, so we won't get any into RNR flow */
	rc = ibv_post_send(res->qp, &sr, &bad_wr);
	if (rc) {
		fprintf(stderr, "failed to post SR\n");
		return 1;
	}
	fprintf(stdout, "Read Request was posted\n");

	return 0;
}

static void resources_init(struct resources *res)
{
	memset(res, 0, sizeof *res);
	res->sock     = -1;
}

static int resources_create(struct resources *res)
{	
	struct ibv_device       **dev_list = NULL;
	struct ibv_qp_init_attr qp_init_attr;
	struct ibv_device 	*ib_dev = NULL;
	int 			i;
	int 			mr_flags = 0;
	int 			cq_size = 0;
	int 			num_devices;

	/* if client side */
	//establish tcp socket connection
	if (config.server_name) {
		res->sock = sock_client_connect(config.server_name, config.tcp_port);
		if (res->sock < 0) {
			fprintf(stderr, "failed to establish TCP connection to server %s, port %d\n", 
				config.server_name, config.tcp_port);
			return -1;
		}
	} else {
		fprintf(stdout, "waiting on port %d for TCP connection\n", config.tcp_port);

		res->sock = sock_daemon_connect(config.tcp_port);
		if (res->sock < 0) {
			fprintf(stderr, "failed to establish TCP connection with client on port %d\n", 
				config.tcp_port);
			return -1;
		}
	}

	fprintf(stdout, "TCP connection was established\n");

	fprintf(stdout, "searching for IB devices in host\n");

	/* get device names in the system */
	dev_list = ibv_get_device_list(&num_devices);
	if (!dev_list) {
		fprintf(stderr, "failed to get IB devices list\n");
		return 1;
	}

	/* if there isn't any IB device in host */
	if (!num_devices) {
		fprintf(stderr, "found %d device(s)\n", num_devices);
		return 1;
	}

	fprintf(stdout, "found %d device(s)\n", num_devices);

	/* search for the specific device we want to work with */
	for (i = 0; i < num_devices; i ++) {
		if (!config.dev_name) {
			config.dev_name = strdup(ibv_get_device_name(dev_list[i])); 
			fprintf(stdout, "device not specified, using first one found: %s\n", config.dev_name);
		}
		if (!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)) {
			ib_dev = dev_list[i];
			break;
		}
	}

	/* if the device wasn't found in host */
	if (!ib_dev) {
		fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
		return 1;
	}

	/* get device handle */
	res->ib_ctx = ibv_open_device(ib_dev);
	if (!res->ib_ctx) {
		fprintf(stderr, "failed to open device %s\n", config.dev_name);
		return 1;
	}

	/* We are now done with device list, free it */
	ibv_free_device_list(dev_list);
	dev_list = NULL;
	ib_dev = NULL;

	/* query port properties  */
	if (ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr)) {
		fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
		return 1;
	}

	/* allocate Protection Domain */
	res->pd = ibv_alloc_pd(res->ib_ctx);
	if (!res->pd) {
		fprintf(stderr, "ibv_alloc_pd failed\n");
		return 1;
	}

	/* each side will send only one WR, so Completion Queue with 1 entry is enough */
	cq_size = 10;
	res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
	if (!res->cq) {
		fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
		return 1;
	}

	res->data_buf = malloc(total_size);
	res->index_buf = malloc(sizeof(struct hashtable));

	memset(res->data_buf, 0, total_size);
	memset(res->index_buf, 0, sizeof(struct hashtable));
	//memcpy(res->data_buf, "hello world", strlen("hello world"));
	
	/* register this memory buffer */
	mr_flags = IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
	res->data_mr = ibv_reg_mr(res->pd, res->data_buf, total_size, mr_flags);
	res->index_mr = ibv_reg_mr(res->pd, res->index_buf, sizeof(struct hashtable), mr_flags);

	/* create the Queue Pair */
	memset(&qp_init_attr, 0, sizeof(qp_init_attr));

	qp_init_attr.qp_type    	= IBV_QPT_RC;
	qp_init_attr.sq_sig_all 	= 1;
	qp_init_attr.send_cq    	= res->cq;
	qp_init_attr.recv_cq    	= res->cq;
	qp_init_attr.cap.max_send_wr  	= 10;
	qp_init_attr.cap.max_recv_wr  	= 10;
	qp_init_attr.cap.max_send_sge 	= 10;
	qp_init_attr.cap.max_recv_sge 	= 10;

	res->qp = ibv_create_qp(res->pd, &qp_init_attr);
	if (!res->qp) {
		fprintf(stderr, "failed to create QP\n");
		return 1;
	}
	fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp->qp_num);

	return 0;
}

static int modify_qp_to_init(struct ibv_qp *qp)
{
	struct ibv_qp_attr 	attr;
	int 			flags;
	int 			rc;


	/* do the following QP transition: RESET -> INIT */
	memset(&attr, 0, sizeof(attr));

	attr.qp_state 	= IBV_QPS_INIT;
	attr.port_num 	= config.ib_port;
	attr.pkey_index = 0;

	attr.qp_access_flags = IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

	flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc) {
		fprintf(stderr, "failed to modify QP state to INIT\n");
		return rc;
	}

	return 0;
}

static int modify_qp_to_rtr(
	struct 	 ibv_qp *qp,
	uint32_t remote_qpn,
	uint16_t dlid)
{
	struct ibv_qp_attr 	attr;
	int 			flags;
	int 			rc;

	/* do the following QP transition: INIT -> RTR */
	memset(&attr, 0, sizeof(attr));

	attr.qp_state 			= IBV_QPS_RTR;
	attr.path_mtu 			= IBV_MTU_256;
	attr.dest_qp_num 		= remote_qpn;
	attr.rq_psn 			= 0;
	attr.max_dest_rd_atomic 	= 0;
	attr.min_rnr_timer 		= 0x12;
	attr.ah_attr.is_global 		= 0;
	attr.ah_attr.dlid 		= dlid;
	attr.ah_attr.sl 		= 0;
	attr.ah_attr.src_path_bits 	= 0;
	attr.ah_attr.port_num 		= config.ib_port;

	flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | 
		IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc) {
		fprintf(stderr, "failed to modify QP state to RTR\n");
		return rc;
	}

	return 0;
}

static int modify_qp_to_rts(struct ibv_qp *qp)
{
	struct ibv_qp_attr 	attr;
	int 			flags;
	int 			rc;


	/* do the following QP transition: RTR -> RTS */
	memset(&attr, 0, sizeof(attr));

	attr.qp_state 		= IBV_QPS_RTS;
	attr.timeout 		= 0x12;
	attr.retry_cnt 		= 6;
	attr.rnr_retry 		= 0;
	attr.sq_psn 		= 0;

	/* the daemon need to be initiator of incoming RDMA Read */
	attr.max_rd_atomic 	= 1;

 	flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | 
		IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

	rc = ibv_modify_qp(qp, &attr, flags);
	if (rc) {
		fprintf(stderr, "failed to modify QP state to RTS\n");
		return rc;
	}

	return 0;
}

static int connect_qp(struct resources *res)
{
	struct cm_con_data_t 	local_con_data;
	struct cm_con_data_t 	remote_con_data;
	struct cm_con_data_t 	tmp_con_data;
	int 			rc;
	/* modify the QP to init */
	rc = modify_qp_to_init(res->qp);
	if (rc) {
		fprintf(stderr, "change QP state to INIT failed\n");
		return rc;
	}
	/* exchange using TCP sockets info required to connect QPs */
	local_con_data.data_addr = htonll((uintptr_t)res->data_buf);
	local_con_data.index_addr = htonll((uintptr_t)res->index_buf);
	local_con_data.data_key   = htonl(res->data_mr->rkey);
	local_con_data.index_key   = htonl(res->index_mr->rkey);

	local_con_data.qp_num = htonl(res->qp->qp_num);
	local_con_data.lid    = htons(res->port_attr.lid);

	fprintf(stdout, "Local LID        = 0x%x\n", res->port_attr.lid);
	fprintf(stdout, "Local data_addr        = 0x%lx\n", (uintptr_t)res->data_buf);
	fprintf(stdout, "Local index_addr        = 0x%lx\n", (uintptr_t)res->index_buf);
	fprintf(stdout, "Local data_key        = 0x%x\n", res->data_mr->rkey);
	fprintf(stdout, "Local index_key        = 0x%x\n", res->index_mr->rkey);

	if (sock_sync_data(res->sock, !config.server_name, sizeof(struct cm_con_data_t), &local_con_data, &tmp_con_data) < 0) {
		fprintf(stderr, "failed to exchange connection data between sides\n");
		return 1;
	}

	remote_con_data.data_addr = ntohll(tmp_con_data.data_addr);
	remote_con_data.index_addr = ntohll(tmp_con_data.index_addr);
	remote_con_data.data_key   = ntohl(tmp_con_data.data_key);
	remote_con_data.index_key   = ntohl(tmp_con_data.index_key);

	remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
	remote_con_data.lid    = ntohs(tmp_con_data.lid);

	res->remote_props = remote_con_data;

	fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
	fprintf(stdout, "Remote LID       = 0x%x\n", remote_con_data.lid);
	fprintf(stdout, "Remote data_addr       = 0x%lx\n", remote_con_data.data_addr);
	fprintf(stdout, "Remote index_addr       = 0x%lx\n", remote_con_data.index_addr);
	fprintf(stdout, "Remote data_key       = 0x%x\n", remote_con_data.data_key);
	fprintf(stdout, "Remote index_key       = 0x%x\n", remote_con_data.index_key);

	/* modify the QP to RTR */
	rc = modify_qp_to_rtr(res->qp, remote_con_data.qp_num, remote_con_data.lid);
	rc = modify_qp_to_rts(res->qp);
	if (rc) {
		fprintf(stderr, "failed to modify QP state from RESET to RTS\n");
		return rc;
	}
	fprintf(stdout, "success to modify OP state to rts\n");
	/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
	if (sock_sync_ready(res->sock, !config.server_name)) {
		fprintf(stderr, "sync after QPs are were moved to RTS\n");
		return 1;
	}

	return 0;
}

static void print_config(void)
{
	fprintf(stdout, " ------------------------------------------------\n");
	fprintf(stdout, " Device name                  : \"%s\"\n", config.dev_name);
	fprintf(stdout, " IB port                      : %u\n", config.ib_port);
	if (config.server_name)
		fprintf(stdout, " IP                           : %s\n", config.server_name);
	fprintf(stdout, " TCP port                     : %u\n", config.tcp_port);
	fprintf(stdout, " ------------------------------------------------\n\n");
}
/*-------------------------------rdma and kv---------------------------------------------*/
const int factor = 1333331;

int cal_hash_index(char *s){
	int len = strlen(s);
	int ret = 0;
	for(int i = 0; i < len; i++){
		ret = (ret * factor + s[i]) % HT_SIZE;
	}
	return ret;
}

int main(int argc, char *argv[])
{
	struct resources 	res;
	fprintf(stdout, "sizeof ht %d\n", (int)sizeof(struct hashtable));
	print_config();

	/* init all of the resources, so cleanup will be easy */
	resources_init(&res);

	/* create resources before using them */
	if (resources_create(&res)) {
		fprintf(stderr, "failed to create resources\n");
		return 1;
	}

	/* connect the QPs */
	if (connect_qp(&res)) {
		fprintf(stderr, "failed to connect QPs\n");
		return 1;
	}

	fprintf(stdout, "connect QP successfully\n");
	fprintf(stdout, "wait for KV operations\n");

	// post_write(&res, 1);
	// if (poll_completion(&res)) {
	// 	fprintf(stderr, "poll completion failed\n");
	// 	return 1;
	// }

	//kv
	char key[8] = "msn";
	char value[1024] = "1234567890";
	//read index

	post_read(&res, 0);
	if (poll_completion(&res)) {
		fprintf(stderr, "poll completion failed\n");
		return 1;
	}
	fprintf(stdout, "read index successfully\n");

	//read data
	post_read(&res, 1);
	if (poll_completion(&res)) {
		fprintf(stderr, "poll completion failed\n");
		return 1;
	}
	fprintf(stdout, "read data successfully\n");

	//cal index
	int h = cal_hash_index(key);
	struct hashtable *ht = (struct hashtable *)res.index_buf;
	memcpy(res.data_buf + loc, value, strlen(value)); //
	ht->exist[h] = 1;
	ht->location[h] = loc;
	memcpy(ht->key[h], key, strlen(key));
	//write data
	post_write(&res, 1);
	if (poll_completion(&res)) {
		fprintf(stderr, "poll completion failed\n");
		return 1;
	}
	fprintf(stdout, "write data successfully\n");

	//write index
	post_write(&res, 0);
	if (poll_completion(&res)) {
		fprintf(stderr, "poll completion failed\n");
		return 1;
	}
	fprintf(stdout, "write index successfully\n");

	//change loc
	loc += strlen(value);

	//read data
	memset(res.data_buf, 0, total_size);
	post_read(&res, 1);
	if (poll_completion(&res)) {
		fprintf(stderr, "poll completion failed\n");
		return 1;
	}
	//read value
	char vv[100];
	memcpy(vv, res.data_buf, strlen(value));
	vv[strlen(value)] = 0;
	printf("get the value: %s\n", vv);

	if (sock_sync_ready(res.sock, !config.server_name)) {
		fprintf(stderr, "sync before end of test\n");
		return 1;
	}
	//fprintf(stdout, "msg %s\n", res.data_buf);
	return 0;
}


