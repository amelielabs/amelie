#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Pipe Pipe;

struct Pipe
{
	ReqQueue queue;
	Channel  src;
	Tr*      tr;
	int      sent;
	Route*   route;
	List     link;
};

static inline Pipe*
pipe_allocate(void)
{
	auto self = (Pipe*)am_malloc(sizeof(Pipe));
	self->tr    = NULL;
	self->sent  = 0;
	self->route = NULL;
	req_queue_init(&self->queue);
	channel_init(&self->src);
	list_init(&self->link);
	return self;
}

static inline void
pipe_free(Pipe* self)
{
	req_queue_free(&self->queue);
	channel_free(&self->src);
	am_free(self);
}

static inline void
pipe_reset(Pipe* self)
{
	self->tr    = NULL;
	self->sent  = 0;
	self->route = NULL;
	req_queue_reset(&self->queue);
	channel_reset(&self->src);
}

static inline void
pipe_shutdown(Pipe* self)
{
	req_queue_shutdown(&self->queue);
}

static inline void
pipe_send(Pipe* self, Req* req)
{
	req_queue_add(&self->queue, req);
	self->sent++;
}

static inline Buf*
pipe_recv(Pipe* self)
{
	auto buf = channel_read(&self->src);
	auto msg = msg_of(buf);
	if (msg->id == MSG_ERROR)
		return buf;
	buf_free(buf);
	return NULL;
}

always_inline static inline Pipe*
pipe_of(Buf* buf)
{
	return *(Pipe**)msg_of(buf)->data;
}

always_inline static inline Tr*
tr_of(Buf* buf)
{
	return *(Tr**)msg_of(buf)->data;
}
