#
# sonata db makefile
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
                 -I. \
                 -Irow \
                 -Itransaction \
                 -Iindex \
                 -Ipartition \
                 -Iwal \
                 -Idb
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libsonata-db.a
OBJECTS        = \
                 row/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 index/tree.o \
                 index/index_tree.o \
                 index/index_hash.o \
                 index/indexate.o \
                 partition/part.o \
                 partition/part_list.o \
                 partition/snapshot.o \
                 partition/snapshot_cursor.o \
                 partition/checkpoint_mgr.o \
                 partition/checkpoint.o \
                 wal/wal.o \
                 wal/wal_cursor.o \
                 db/schema_mgr.o \
                 db/table_mgr.o \
                 db/table_index.o \
                 db/view_mgr.o \
                 db/db.o \
                 db/db_catalog.o \
                 db/recover_checkpoint.o \
                 db/recover.o \
                 db/cascade.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
