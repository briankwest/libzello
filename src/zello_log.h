#ifndef LIBZELLO_LOG_H
#define LIBZELLO_LOG_H

#include "libzello/zello.h"

void zello_log(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

#define ZLOG_D(...) zello_log(ZELLO_LOG_DEBUG, __VA_ARGS__)
#define ZLOG_I(...) zello_log(ZELLO_LOG_INFO,  __VA_ARGS__)
#define ZLOG_W(...) zello_log(ZELLO_LOG_WARN,  __VA_ARGS__)
#define ZLOG_E(...) zello_log(ZELLO_LOG_ERROR, __VA_ARGS__)

#endif
