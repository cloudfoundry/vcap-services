#include "params.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <pthread.h>

#include "rquota.h"

static unsigned long long dir_quota = 0;
unsigned long long dir_usage = 0;
unsigned int request_du = 1;
char *fss_id = NULL;
char *fss_node_id = NULL;
char *redis_ip = NULL;
unsigned int redis_port = 0;
char *redis_passwd = NULL;

static void dpx_fullpath(char fpath[PATH_MAX], const char *path) {
  strcpy(fpath, DPX_DATA->rootdir);
  strncat(fpath, path, PATH_MAX);
}

int dpx_getattr(const char *path, struct stat *statbuf) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return lstat(fpath, statbuf) ? -errno : 0;
}

int dpx_readlink(const char *path, char *link, size_t size) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  ret = readlink(fpath, link, size - 1);
  if (ret < 0)
    ret = -errno;

  link[ret] = '\0';
  return ret;
}

int dpx_mknod(const char *path, mode_t mode, dev_t dev) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return mknod(fpath, mode, dev)? -errno : 0;
}

int dpx_mkdir(const char *path, mode_t mode) {
  int ret = 0;
  char fpath[PATH_MAX];

  if(dir_usage > dir_quota) return -EDQUOT;
  dpx_fullpath(fpath, path);
  ret = mkdir(fpath, mode);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_unlink(const char *path) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);
  ret = unlink(fpath);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_rmdir(const char *path) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);
  ret = rmdir(fpath);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_symlink(const char *from, const char *to) {
  int ret = 0;
  char fpath[PATH_MAX];

  if(dir_usage > dir_quota) return -EDQUOT;
  dpx_fullpath(fpath, to);
  ret = symlink(from, fpath);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_rename(const char *from, const char *to) {
  char fpath[PATH_MAX];
  char fnewpath[PATH_MAX];

  dpx_fullpath(fpath, from);
  dpx_fullpath(fnewpath, to);

  return rename(fpath, fnewpath)? -errno : 0;
}

int dpx_link(const char *from, const char *to) {
  char fpath[PATH_MAX];
  char fnewpath[PATH_MAX];

  dpx_fullpath(fpath, from);
  dpx_fullpath(fnewpath, to);

  return link(fpath, fnewpath)? -errno : 0;
}

int dpx_chmod(const char *path, mode_t mode) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return chmod(fpath, mode)? -errno : 0;
}

int dpx_chown(const char *path, uid_t uid, gid_t gid) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return chown(fpath, uid, gid)? -errno : 0;
}

int dpx_truncate(const char *path, off_t offset) {
  int ret = 0;
  char fpath[PATH_MAX];
  struct stat sb;

  dpx_fullpath(fpath, path);

  ret = lstat(fpath, &sb);
  if(ret < 0) return -errno;
  if(dir_usage >= sb.st_size - offset
      && dir_usage - sb.st_size + offset > dir_quota)
    return -EDQUOT;

  ret = truncate(fpath, offset);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi) {
  int ret = 0;
  struct stat sb;

  ret = fstat(fi->fh, &sb);
  if(ret < 0) return -errno;
  if(dir_usage >= sb.st_size - offset
    && dir_usage - sb.st_size + offset > dir_quota)
    return -EDQUOT;

  ret = ftruncate(fi->fh, offset);
  if(ret < 0) return -errno;
  request_du = 1;

  return 0;
}

int dpx_utime(const char *path, struct utimbuf *ubuf) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return utime(fpath, ubuf)? -errno : 0;
}

int dpx_open(const char *path, struct fuse_file_info *fi) {
  int ret = 0;
  char fpath[PATH_MAX];

  if(fi->flags & O_CREAT) {
    if(dir_usage > dir_quota) return -EDQUOT;
    request_du = 1;
  }

  dpx_fullpath(fpath, path);
  ret = open(fpath, fi->flags);
  if(ret < 0) return -errno;


  fi->fh = ret;

  return 0;
}

int dpx_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
  int ret = 0;
  char fpath[PATH_MAX];

  if(dir_usage > dir_quota) return -EDQUOT;
  dpx_fullpath(fpath, path);
  ret = open(fpath, fi->flags, mode);
  if(ret < 0) return -errno;

  request_du = 1;
  fi->fh = ret;

  return 0;
}

int dpx_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
  int ret = 0;

  ret = pread(fi->fh, buf, size, offset);

  return ret < 0 ? -errno : ret;
}

int dpx_write(const char *path, const char *buf, size_t size, off_t offset,
    struct fuse_file_info *fi) {
  int ret = 0;

  if(dir_usage + size > dir_quota)
    return -EDQUOT;

  ret = pwrite(fi->fh, buf, size, offset);
  if(ret < 0) return -errno;

  request_du = 1;

  return ret;
}

int dpx_statfs(const char *path, struct statvfs *statv) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return statvfs(fpath, statv)? -errno : 0;
}

int dpx_flush(const char *path, struct fuse_file_info *fi) {
  return 0;
}

int dpx_release(const char *path, struct fuse_file_info *fi) {
  return close(fi->fh)? -errno : 0;
}

int dpx_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
  int ret = 0;

  if (datasync)
    ret = fdatasync(fi->fh);
  else
    ret = fsync(fi->fh);

  if(ret < 0) return -errno;

  return 0;
}

int dpx_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return lsetxattr(fpath, name, value, size, flags)? -errno : 0;
}

int dpx_getxattr(const char *path, const char *name, char *value, size_t size) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);
  ret = lgetxattr(fpath, name, value, size);

  return ret < 0 ? -errno : ret;
}

int dpx_listxattr(const char *path, char *list, size_t size) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);
  ret = llistxattr(fpath, list, size);

  return ret < 0 ? -errno : ret;
}

int dpx_removexattr(const char *path, const char *name) {
  int ret = 0;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  ret = lremovexattr(fpath, name);

  return ret < 0 ? -errno : ret;
}

int dpx_opendir(const char *path, struct fuse_file_info *fi) {
  DIR *dp;
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);
  dp = opendir(fpath);
  if (dp == NULL) return -errno;
  fi->fh = (intptr_t) dp;

  return 0;
}

int dpx_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  struct dirent *de = NULL;

  while ((de = readdir ((DIR *) fi->fh)) != NULL) {
    struct stat st;
    memset (&st, 0, sizeof (struct stat));
    st.st_ino = de->d_ino;
    st.st_mode = de->d_type << 12;

    if (filler (buf, de->d_name, &st, 0))
      break;
  }

  return 0;
}

int dpx_releasedir(const char *path, struct fuse_file_info *fi) {
  return closedir((DIR *) (uintptr_t) fi->fh) ? -errno : 0;
}

void *dpx_init(struct fuse_conn_info *conn) {
  pthread_t rquota;
  pthread_create(&rquota, NULL, rquota_job, NULL);

  return DPX_DATA;
}

void dpx_destroy(void *userdata) {
}

int dpx_access(const char *path, int mask) {
  char fpath[PATH_MAX];

  dpx_fullpath(fpath, path);

  return access(fpath, mask) ? -errno : 0;
}

int dpx_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi) {
  return fstat(fi->fh, statbuf) ? -errno : 0;
}

struct fuse_operations dpx_oper = {
  .getattr = dpx_getattr,
  .readlink = dpx_readlink,
  .getdir = NULL,
  .mknod = dpx_mknod,
  .mkdir = dpx_mkdir,
  .unlink = dpx_unlink,
  .rmdir = dpx_rmdir,
  .symlink = dpx_symlink,
  .rename = dpx_rename,
  .link = dpx_link,
  .chmod = dpx_chmod,
  .chown = dpx_chown,
  .truncate = dpx_truncate,
  .utime = dpx_utime,
  .open = dpx_open,
  .read = dpx_read,
  .write = dpx_write,
  .statfs = dpx_statfs,
  .flush = dpx_flush,
  .release = dpx_release,
  .fsync = dpx_fsync,
  .setxattr = dpx_setxattr,
  .getxattr = dpx_getxattr,
  .listxattr = dpx_listxattr,
  .removexattr = dpx_removexattr,
  .opendir = dpx_opendir,
  .readdir = dpx_readdir,
  .releasedir = dpx_releasedir,
  .init = dpx_init,
  .destroy = dpx_destroy,
  .access = dpx_access,
  .create = dpx_create,
  .ftruncate = dpx_ftruncate,
  .fgetattr = dpx_fgetattr
};

int main(int argc, char *argv[]) {
  char *root;
  char *start, *end;
  struct dpx_state *dpx_data;

  root          = getenv("ROOT_DIR");
  fss_id        = getenv("FSS_ID");
  fss_node_id   = getenv("FSS_NODE_ID");
  start         = getenv("QUOTA");
  dir_quota     = strtoll(start, &end, 10);
  if(!*start || *end) {
    fprintf(stderr, "Wrong quota %s\n", start);
    abort();
  }
  dir_quota     = dir_quota * 1024 * 1024;
  redis_ip      = getenv("REDIS_IP");
  start         = getenv("REDIS_PORT");
  redis_port    = strtol(start, &end, 10);
  if(!*start || *end) {
    fprintf(stderr, "Wrong redis_port %s\n", start);
    abort();
  }
  redis_passwd  = getenv("REDIS_PASSWD");

  dpx_data = calloc(sizeof(struct dpx_state), 1);

  if (dpx_data == NULL) {
    perror("main calloc");
    abort();
  }
  dpx_data->rootdir = realpath(root, NULL);

  fprintf(stderr, "env: %s %s %s %llu %s %d %s\n", root, fss_id, fss_node_id, dir_quota, redis_ip, redis_port, redis_passwd);

  return fuse_main(argc, argv, &dpx_oper, dpx_data);
}
