#
# sonata vm makefile
#
CC             = clang
AR             = ar
RM             = rm
CFLAGS_OPTS    = -Wall -Wextra -g -fPIC -O0
CFLAGS_INCLUDE = -I../runtime \
                 -I../runtime/runtime \
                 -I../runtime/io \
                 -I../runtime/lib \
                 -I../runtime/data \
                 -I../runtime/config \
                 -I../server \
                 -I../server/auth \
                 -I../server/http \
                 -I../server/client \
                 -I../server/server \
                 -I../db \
                 -I../db/def \
                 -I../db/transaction \
                 -I../db/index \
                 -I../db/partition \
                 -I../db/wal \
                 -I../db/db \
                 -I. \
                 -Ivalue \
                 -Iaggr \
                 -Iexecutor \
                 -Ivm
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libsonata-vm.a
OBJECTS        = \
                 value/update.o \
                 aggr/set.o \
                 aggr/merge.o \
                 aggr/count.o \
                 aggr/sum.o \
                 aggr/avg.o \
                 aggr/min.o \
                 aggr/max.o \
                 aggr/aggrs.o \
                 aggr/group.o \
                 aggr/group_merge.o \
                 executor/executor.o \
                 vm/op.o \
                 vm/vm.o \
                 vm/command.o \
                 vm/func.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
