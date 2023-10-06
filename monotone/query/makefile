#
# monotone query makefile
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
                 -I../db/transaction \
                 -I../db/storage \
                 -I../db/wal \
                 -I../db/db \
                 -I. \
                 -Ivalue \
                 -Iaggr \
                 -Ivm \
                 -Iparser \
                 -Icompiler
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-query.a
OBJECTS        = \
                 value/update.o \
                 aggr/set.o \
                 aggr/aggrs.o \
                 aggr/count.o \
                 aggr/sum.o \
                 aggr/avg.o \
                 aggr/group.o \
                 vm/op.o \
                 vm/code.o \
                 vm/vm.o \
                 vm/command.o \
                 vm/func.o \
                 vm/func_debug.o \
                 vm/callable.o \
                 parser/keywords.o \
                 parser/lex.o \
                 parser/parser.o \
                 compiler/compiler.o

#                compiler/keyword.o \
                 compiler/expr.o \
#                compiler/expr_parse.o \
                 compiler/expr_generate.o \
                 compiler/expr_type.o \
                 compiler/scan.o \
                 compiler/select.o \
                 compiler/stmt_from.o \
                 compiler/stmt.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f parser/grammar.c
	@$(RM) -f parser/grammar.h
	@$(RM) -f $(OBJECTS) $(LIB)
