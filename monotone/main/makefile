#
# monotone main makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -fPIC -O0
CFLAGS_INCLUDE = -I../runtime \
                 -I../runtime/runtime \
                 -I../runtime/io \
                 -I../runtime/data \
                 -I../runtime/lib \
                 -I../runtime/config \
                 -I../server \
                 -I../server/auth \
                 -I../server/client \
                 -I../server/server \
                 -I../db \
                 -I../db/schema \
                 -I../db/mvcc \
                 -I../db/storage \
                 -I../db/wal \
                 -I../db/db \
                 -I../cluster \
                 -I../cluster/shard \
                 -I../cluster/session \
                 -I../cluster/hub \
                 -I. \
                 -Icore \
                 -Imain \
                 -Iapi
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-main.a
OBJECTS        = \
                 core/core.o \
                 main/main.o \
                 api/monotone.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
