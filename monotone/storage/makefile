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
                 -Ischema \
                 -Imvcc \
                 -Iengine \
                 -Istorage
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-storage.a
OBJECTS        = \
                 schema/row.o \
                 mvcc/handle_mgr.o \
                 mvcc/heap.o \
                 mvcc/mvcc.o \
                 engine/compact.o \
                 engine/service.o \
                 engine/engine.o \
                 engine/engine_recover.o \
                 engine/engine_stats.o \
                 engine/compaction.o \
                 storage/storage.o \
                 storage/storage_iterator.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
