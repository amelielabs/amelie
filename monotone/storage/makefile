#
# monotone storage makefile
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
                 -Iindex \
                 -Istorage
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-storage.a
OBJECTS        = \
                 def/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 index/tree.o \
                 storage/snapshot.o \
                 storage/snapshot_cursor.o \
                 storage/part.o \
                 storage/storage.o \
                 storage/storage_mgr.o \
                 storage/checkpoint.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
