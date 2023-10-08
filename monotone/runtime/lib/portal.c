
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>

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
