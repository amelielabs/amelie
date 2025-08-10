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

typedef struct Request Request;

typedef enum
{
	REQUEST_UNDEF,
	REQUEST_CONNECT,
	REQUEST_DISCONNECT,
	REQUEST_EXECUTE
} RequestType;

struct Request
{
	Mutex       lock;
	CondVar     cond_var;
	RequestType type;
	bool        complete;
	bool        error;
	Buf         content;
	Str         cmd;
	void      (*on_complete)(void*);
	void*       on_complete_arg;
	List        link;
};

static inline Request*
request_allocate(void)
{
	auto self = (Request*)am_malloc(sizeof(Request));
	self->type            = REQUEST_UNDEF;
	self->complete        = false;
	self->error           = false;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->link);
	return self;
}

static inline void
request_free(Request* self)
{
	buf_free(&self->content);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
	am_free(self);
}

static inline void
request_reset(Request* self)
{
	self->type            = REQUEST_UNDEF;
	self->complete        = false;
	self->error           = false;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	str_init(&self->cmd);
	buf_reset(&self->content);
}

static inline void
request_complete(Request* self, bool error)
{
	mutex_lock(&self->lock);
	self->complete = true;
	self->error    = error;
	if (self->on_complete)
		self->on_complete(self->on_complete_arg);
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline bool
request_wait(Request* self, uint32_t time_ms)
{
	mutex_lock(&self->lock);
	auto complete = self->complete;
	if (! self->complete)
	{
		if (time_ms > 0)
			cond_var_timedwait(&self->cond_var, &self->lock, time_ms);
		complete = self->complete;
	}
	mutex_unlock(&self->lock);
	return complete;
}
