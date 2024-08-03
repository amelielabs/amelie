#
# amelie vm makefile
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
                 -I../server/node \
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
                 -I. \
                 -Ivalue \
                 -Iaggr \
                 -Iexecutor \
                 -Ivm \
                 -Ifunc
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libamelie-vm.a
OBJECTS        = \
                 func/fn_system.o \
                 func/fn_cast.o \
                 func/fn_null.o \
                 func/fn_object.o \
                 func/fn_string.o \
                 func/fn_math.o \
                 func/fn_misc.o \
                 func/fn_time.o \
                 func/fn.o \
                 value/update.o \
                 value/like.o \
                 aggr/set.o \
                 aggr/any.o \
                 aggr/all.o \
                 aggr/merge.o \
                 aggr/count.o \
                 aggr/sum.o \
                 aggr/avg.o \
                 aggr/min.o \
                 aggr/max.o \
                 aggr/lambda.o \
                 aggr/aggrs.o \
                 aggr/group.o \
                 aggr/group_merge.o \
                 executor/executor.o \
                 vm/op.o \
                 vm/vm.o \
                 vm/command.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
