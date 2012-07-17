#include "params.h"

#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>

#include "hiredis.h"

void *rquota_job() {
  redisContext *c;
  redisReply *reply;

  struct timeval timeout = { 1, 500000 }; // 1.5 seconds

  for(;;) {
    sleep(1);

    c = redisConnectWithTimeout(redis_ip, redis_port, timeout);
    if (c->err) {
      continue;
    }
    reply= redisCommand(c, "AUTH %s", redis_passwd);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
      continue;
    }
    freeReplyObject(reply);

    if(request_du) {
      reply = redisCommand(c,"HSET fss_req:%s %s %d", fss_node_id, fss_id, 1);
      freeReplyObject(reply);
      request_du = 0;
    }

    reply = redisCommand(c,"HGET fss_usage:%s %s", fss_node_id, fss_id);
    if(reply && reply->type == REDIS_REPLY_INTEGER) {
      dir_usage = reply->integer;
    }
    freeReplyObject(reply);

    redisFree(c);
  }
}
