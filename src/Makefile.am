lib_LTLIBRARIES = libzello.la

libzello_la_SOURCES = \
	zello_client.c \
	zello_ws.c \
	zello_proto.c \
	zello_codec.c \
	zello_log.c \
	zello_internal.h \
	zello_ws.h \
	zello_proto_priv.h \
	zello_codec.h \
	zello_log.h

libzello_la_CFLAGS = \
	-std=c11 -D_GNU_SOURCE \
	-Wall -Wextra \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/src \
	-DLIBZELLO_GIT_HASH=\"$(GIT_HASH)\" \
	$(LIBWEBSOCKETS_CFLAGS) \
	$(OPUS_CFLAGS) \
	$(CJSON_CFLAGS)

libzello_la_LDFLAGS = -version-info 0:0:0

libzello_la_LIBADD = \
	$(LIBWEBSOCKETS_LIBS) \
	$(OPUS_LIBS) \
	$(CJSON_LIBS) \
	-lpthread \
	-lm
