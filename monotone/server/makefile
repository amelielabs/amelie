#
# monotone server makefile
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
                 -Iauth \
                 -Iclient \
                 -Iserver
CFLAGS         = $(CFLAGS_INCLUDE) $(CFLAGS_OPTS)
LIB            = libmonotone-server.a
OBJECTS        = \
                 auth/auth.o \
                 client/client.o \
                 client/relay.o \
                 server/hub.o \
                 server/server.o
$(LIB): clean $(OBJECTS)
	@$(AR) cr $(LIB) $(OBJECTS)
	@echo $(AR) $(LIB)
.c.o:
	@$(CC) $(CFLAGS) -c $< -o $@
	@echo $(CC) -c $<
clean:
	@$(RM) -f $(OBJECTS) $(LIB)
