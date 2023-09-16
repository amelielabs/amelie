#
# monotone runtime makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -fPIC -O0
CFLAGS_INCLUDE = -I. \
                 -Iruntime \
                 -Iio \
                 -Idata \
                 -Ilib \
                 -Iconfig
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-runtime.a
OBJECTS        = \
                 runtime/timer_mgr.o \
                 runtime/poller.o \
                 runtime/notify.o \
                 runtime/context_stack.o \
                 runtime/context.o \
                 runtime/channel.o \
                 runtime/task.o \
                 io/vfs.o \
                 io/logger.o \
                 io/tls_lib.o \
                 io/tls_context.o \
                 io/tls.o \
                 io/socket.o \
                 io/tcp.o \
                 io/listen.o \
                 io/resolver.o \
                 data/json.o \
                 data/to_string.o \
                 lib/rbtree.o \
                 lib/sha1.o \
                 lib/crc.o \
                 lib/uuid_mgr.o \
                 lib/compression.o \
                 lib/uri.o \
                 lib/lex.o \
                 lib/portal.o \
                 config/config.o \
                 config/config_state.o \
                 config/catalog_mgr.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
