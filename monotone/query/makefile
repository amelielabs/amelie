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
                 -I../storage \
                 -I../storage/def \
                 -I../storage/transaction \
                 -I../storage/index \
                 -I../storage/storage \
                 -I../db \
                 -I../db/wal \
                 -I../db/db \
                 -I. \
                 -Ivalue \
                 -Iaggr \
                 -Irequest \
                 -Ivm \
                 -Iparser \
                 -Isemantic \
                 -Icompiler
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-query.a
OBJECTS        = \
                 value/update.o \
                 aggr/set.o \
                 aggr/merge.o \
                 aggr/aggrs.o \
                 aggr/count.o \
                 aggr/sum.o \
                 aggr/avg.o \
                 aggr/min.o \
                 aggr/max.o \
                 aggr/group.o \
                 aggr/group_merge.o \
                 request/dispatch.o \
                 request/dispatch_io.o \
                 vm/op.o \
                 vm/func.o \
                 vm/func_debug.o \
                 vm/vm.o \
                 vm/command.o \
                 parser/keyword.o \
                 parser/lex.o \
                 parser/parse_expr.o \
                 parser/parse_user.o \
                 parser/parse_schema.o \
                 parser/parse_table.o \
                 parser/parse_view.o \
                 parser/parse_insert.o \
                 parser/parse_update.o \
                 parser/parse_delete.o \
                 parser/parse_from.o \
                 parser/parse_select.o \
                 parser/parse.o \
                 parser/parser.o \
                 semantic/plan.o \
                 compiler/compiler.o \
                 compiler/scan.o \
                 compiler/emit_expr.o \
                 compiler/emit_insert.o \
                 compiler/emit_update.o \
                 compiler/emit_delete.o \
                 compiler/emit_upsert.o \
                 compiler/emit_select.o \
                 compiler/pushdown.o
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
