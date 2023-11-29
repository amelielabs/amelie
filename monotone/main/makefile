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
                 -I../storage \
                 -I../storage/def \
                 -I../storage/transaction \
                 -I../storage/index \
                 -I../storage/storage \
                 -I../db \
                 -I../db/wal \
                 -I../db/db \
                 -I../query \
                 -I../query/value \
                 -I../query/aggr \
                 -I../query/request \
                 -I../query/vm \
                 -I../query/parser \
                 -I../query/semantic \
                 -I../query/compiler \
                 -I. \
                 -Ishard \
                 -Ihub \
                 -Isession \
                 -Icore \
                 -Imain \
                 -Iapi
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-main.a
OBJECTS        = \
                 shard/shard.o \
                 hub/hub.o \
                 hub/client.o \
                 session/explain.o \
                 session/session.o \
                 session/lock.o \
                 session/utility.o \
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
