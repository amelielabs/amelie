#
# sonata db makefile
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
                 -I. \
                 -Idef \
                 -Itransaction \
                 -Iindex \
                 -Istorage \
                 -Iwal \
                 -Idb
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libsonata-db.a
OBJECTS        = \
                 def/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 index/ttree.o \
                 index/tree.o \
                 storage/snapshot.o \
                 storage/snapshot_cursor.o \
                 storage/snapshot_mgr.o \
                 storage/storage.o \
                 storage/storage_mgr.o \
                 storage/checkpoint.o \
                 db/schema_mgr.o \
                 db/table_mgr.o \
                 db/view_mgr.o \
                 db/db.o \
                 db/db_catalog.o \
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
