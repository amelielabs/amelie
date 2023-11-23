#
# monotone db makefile
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
                 -I. \
                 -Idef \
                 -Itransaction \
                 -Istorage \
                 -Isnapshot \
                 -Iwal \
                 -Idb
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-db.a
OBJECTS        = \
                 def/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 storage/tree.o \
                 storage/storage.o \
                 storage/storage_mgr.o \
                 snapshot/snapshot_file.o \
                 snapshot/snapshot_cursor.o \
                 snapshot/snapshot_mgr.o \
                 snapshot/snapshot.o \
                 wal/wal_store.o \
                 wal/wal_store_open.o \
                 wal/wal.o \
                 wal/wal_cursor.o \
                 wal/debug.o \
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
