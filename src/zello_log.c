#include "zello_log.h"
#include "libzello/zello_client.h"

#include <stdarg.h>
#include <stdio.h>

static zello_log_fn  g_log_fn    = NULL;
static void         *g_log_ud    = NULL;
static int           g_log_level = ZELLO_LOG_INFO;

void zello_set_log_callback(zello_log_fn fn, void *ud)
{
    g_log_fn = fn;
    g_log_ud = ud;
}

void zello_set_log_level(int level)
{
    g_log_level = level;
}

void zello_log(int level, const char *fmt, ...)
{
    if (level < g_log_level) return;

    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (g_log_fn) {
        g_log_fn(level, buf, g_log_ud);
    } else {
        static const char *tag[] = {"D", "I", "W", "E"};
        const char *t = (level >= 0 && level <= 3) ? tag[level] : "?";
        fprintf(stderr, "[zello %s] %s\n", t, buf);
    }
}
