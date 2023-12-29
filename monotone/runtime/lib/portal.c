
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>

void
portal_to_channel(Portal* self, Buf* buf)
{
	Channel* dest = self->arg;
	channel_write(dest, buf);
}

void
portal_to_tcp(Portal* self, Buf* buf)
{
	Tcp* dest = self->arg;
	tcp_send(dest, buf);
}

void
portal_to_free(Portal* self, Buf* buf)
{
	unused(self);
	buf_free(buf);
}
