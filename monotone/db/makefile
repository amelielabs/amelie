#
# indigo db makefile
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
                 -I../storage \
                 -I../storage/def \
                 -I../storage/transaction \
                 -I../storage/index \
                 -I../storage/storage \
                 -I. \
                 -Iwal \
                 -Idb
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libindigo-db.a
OBJECTS        = \
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
