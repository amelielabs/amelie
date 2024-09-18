#
# amelie compiler makefile
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
                 -I../server/auth \
                 -I../server/http \
                 -I../server/client \
                 -I../server/server \
                 -I../db \
                 -I../db/row \
                 -I../db/transaction \
                 -I../db/index \
                 -I../db/partition \
                 -I../db/checkpoint \
                 -I../db/wal \
                 -I../db/db \
                 -I../vm \
                 -I../vm/value \
                 -I../vm/aggr \
                 -I../vm/executor \
                 -I../vm/vm \
                 -I../vm/func \
                 -I. \
                 -Iparser \
                 -Iplanner \
                 -Icompiler
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libamelie-compiler.a
OBJECTS        = \
                 parser/keyword.o \
                 parser/lex.o \
                 parser/parse_system.o \
                 parser/parse_user.o \
                 parser/parse_replica.o \
                 parser/parse_repl.o \
                 parser/parse_node.o \
                 parser/parse_schema.o \
                 parser/parse_table.o \
                 parser/parse_index.o \
                 parser/parse_view.o \
                 parser/parse_cte.o \
                 parser/parse_expr.o \
                 parser/parse_insert.o \
                 parser/parse_update.o \
                 parser/parse_delete.o \
                 parser/parse_from.o \
                 parser/parse_select.o \
                 parser/parse_watch.o \
                 parser/parse.o \
                 parser/parser.o \
                 planner/path.o \
                 planner/planner.o \
                 compiler/compiler.o \
                 compiler/scan.o \
                 compiler/emit_expr.o \
                 compiler/emit_insert.o \
                 compiler/emit_update.o \
                 compiler/emit_delete.o \
                 compiler/emit_upsert.o \
                 compiler/emit_select.o \
                 compiler/emit_watch.o \
                 compiler/pushdown.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
