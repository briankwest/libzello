AC_PREREQ([2.69])
AC_INIT([libzello], [0.1.6], [https://github.com/briankwest/libzello/issues])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIRS([m4])
AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AM_PROG_AR
LT_INIT

AC_PROG_CC
AC_PROG_INSTALL

PKG_CHECK_MODULES([OPENSSL], [openssl], [],
    [AC_MSG_ERROR([OpenSSL is required (apt install libssl-dev)])])
PKG_CHECK_MODULES([OPUS], [opus], [],
    [AC_MSG_ERROR([libopus is required (apt install libopus-dev)])])
PKG_CHECK_MODULES([CJSON], [libcjson], [],
    [AC_MSG_ERROR([libcjson is required (apt install libcjson-dev)])])

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([
    Makefile
    src/Makefile
    include/Makefile
    tests/Makefile
    examples/Makefile
    libzello.pc
])
AC_OUTPUT

echo
echo "libzello $PACKAGE_VERSION configured."
echo "  openssl       : yes"
echo "  opus          : yes"
echo "  cjson         : yes"
echo
