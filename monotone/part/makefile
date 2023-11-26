#
# monotone partition makefile
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
                 -Isnapshot \
                 -Istorage \
                 -Ipart
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-part.a
OBJECTS        = \
                 def/row.o \
                 transaction/handle_mgr.o \
                 transaction/commit.o \
                 snapshot/snapshot.o \
                 snapshot/snapshot_cursor.o \
                 storage/tree.o \
                 storage/storage.o \
                 part/part_mgr.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
