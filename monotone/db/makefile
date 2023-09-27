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
                 -Ischema \
                 -Itransaction \
                 -Istorage \
                 -Iwal \
                 -Idb
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-db.a
OBJECTS        = \
                 schema/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 storage/index_tree.o \
                 storage/storage.o \
                 storage/storage_mgr.o \
                 wal/wal_store.o \
                 wal/wal_store_open.o \
                 wal/wal.o \
                 wal/wal_cursor.o \
                 wal/debug.o \
                 db/table_mgr.o \
                 db/meta_mgr.o \
                 db/db.o \
                 db/db_catalog.o \
                 db/recover.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
