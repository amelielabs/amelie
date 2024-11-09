#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct Pipe Pipe;

typedef enum
{
	PIPE_OPEN,
	PIPE_CLOSE
} PipeState;

struct Pipe
{
	PipeState state;
	ReqQueue  queue;
	Channel   src;
	Tr*       tr;
	int       last_stmt;
	Route*    route;
	List      link;
};

static inline Pipe*
pipe_allocate(void)
{
	auto self = (Pipe*)am_malloc(sizeof(Pipe));
	self->state     = PIPE_OPEN;
	self->tr        = NULL;
	self->last_stmt = -1;
	self->route     = NULL;
	req_queue_init(&self->queue);
	channel_init(&self->src);
	list_init(&self->link);
	return self;
}

static inline void
pipe_free(Pipe* self)
{
	req_queue_free(&self->queue);
	channel_detach(&self->src);
	channel_free(&self->src);
	am_free(self);
}

static inline void
pipe_reset(Pipe* self)
{
	self->state     = PIPE_OPEN;
	self->tr        = NULL;
	self->last_stmt = -1;
	self->route     = NULL;
	req_queue_reset(&self->queue);
	channel_reset(&self->src);
}

static inline void
pipe_send(Pipe* self, Req* req, int stmt, bool shutdown)
{
	req->shutdown = shutdown;
	req_queue_add(&self->queue, req);
	self->last_stmt = stmt;
}

static inline Buf*
pipe_recv(Pipe* self)
{
	auto buf = channel_read(&self->src, -1);
	auto msg = msg_of(buf);
	switch (msg->id) {
	case MSG_OK:
		buf_free(buf);
		return NULL;
	case MSG_READY:
		buf_free(buf);
		self->state = PIPE_CLOSE;
		return NULL;
	case MSG_ERROR:
		self->state = PIPE_CLOSE;
		break;
	}
	return buf;
}

static inline void
pipe_close(Pipe* self)
{
	while (self->state != PIPE_CLOSE)
	{
		auto error = pipe_recv(self);
		if (error)
			buf_free(error);
	}
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
