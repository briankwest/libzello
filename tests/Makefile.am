AM_CFLAGS = -std=c11 -Wall -Wextra -D_GNU_SOURCE -I$(top_srcdir)/include \
            $(OPUS_CFLAGS)

check_PROGRAMS = test_codec_roundtrip

test_codec_roundtrip_SOURCES = test_codec_roundtrip.c
test_codec_roundtrip_LDADD   = $(OPUS_LIBS) -lm
