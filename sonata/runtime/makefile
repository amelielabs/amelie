#
# sonata runtime makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -O0
CFLAGS_INCLUDE = -I. \
                 -Iruntime \
                 -Iio \
                 -Ilib \
                 -Idata \
                 -Iconfig
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libsonata-runtime.a
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
                 lib/rbtree.o \
                 lib/random.o \
                 lib/uuid.o \
                 lib/crc.o \
                 data/json.o \
                 data/json_export.o \
                 config/config.o \
                 config/catalog_mgr.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
