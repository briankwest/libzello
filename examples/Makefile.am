noinst_PROGRAMS = zello_listen zello_tone zello_cli

zello_listen_SOURCES = zello_listen.c
zello_listen_CFLAGS  = -std=c11 -Wall -Wextra -I$(top_srcdir)/include
zello_listen_LDADD   = $(top_builddir)/src/libzello.la

zello_tone_SOURCES = zello_tone.c
zello_tone_CFLAGS  = -std=c11 -Wall -Wextra -I$(top_srcdir)/include
zello_tone_LDADD   = $(top_builddir)/src/libzello.la -lm

zello_cli_SOURCES = zello_cli.c linenoise.c linenoise.h
zello_cli_CFLAGS  = -std=c11 -Wall -D_GNU_SOURCE -I$(top_srcdir)/include
zello_cli_LDADD   = $(top_builddir)/src/libzello.la -lm
