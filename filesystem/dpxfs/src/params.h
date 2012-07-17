#ifndef _PARAMS_H_
#define _PARAMS_H_

#define FUSE_USE_VERSION 26

#define _XOPEN_SOURCE 600

#include <limits.h>
#include <stdio.h>
struct dpx_state {
  char *rootdir;
};
#define DPX_DATA ((struct dpx_state *) fuse_get_context()->private_data)

extern unsigned long long dir_usage;
extern unsigned int request_du;
extern char *fss_id;
extern char *fss_node_id;
extern char *redis_ip;
extern unsigned int redis_port;
extern char *redis_passwd;

#endif
