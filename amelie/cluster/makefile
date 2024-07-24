#
# amelie cluster makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -O0
CFLAGS_INCLUDE = -I../runtime \
                 -I../runtime/runtime \
                 -I../runtime/io \
                 -I../runtime/lib \
                 -I../runtime/data \
                 -I../runtime/config \
                 -I../server \
                 -I../server/user \
                 -I../server/http \
                 -I../server/client \
                 -I../server/server \
                 -I../db \
                 -I../db/row \
                 -I../db/transaction \
                 -I../db/index \
                 -I../db/partition \
                 -I../db/wal \
                 -I../db/db \
                 -I../vm \
                 -I../vm/value \
                 -I../vm/aggr \
                 -I../vm/executor \
                 -I../vm/vm \
                 -I../compiler \
                 -I../compiler/parser \
                 -I../compiler/planner \
                 -I../compiler/compiler \
                 -I. \
                 -Ibackup \
                 -Irepl \
                 -Icluster
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libamelie-cluster.a
OBJECTS        = \
                 backup/backup.o \
                 backup/restore.o \
                 repl/streamer.o \
                 repl/replica_mgr.o \
                 repl/primary.o \
                 repl/repl.o \
                 cluster/node.o \
                 cluster/cluster.o \
                 cluster/build.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
