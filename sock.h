#ifndef SOCK_H
#define SOCK_H

int sock_daemon_connect(int port);
int sock_client_connect(const char *server_name, int port);
int sock_sync_data(int sock_fd, int is_daemon, size_t size, const void *out_buf, void *in_buf);
int sock_sync_ready(int sock_fd, int is_daemon);

#endif /* SOCK_H */
