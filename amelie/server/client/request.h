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
	REQUEST_CONNECT,
	REQUEST_DISCONNECT,
	REQUEST_EXECUTE
} RequestType;

typedef void (*RequestNotify)(void*);

struct Request
{
	Mutex         lock;
	CondVar       cond_var;
	RequestType   type;
	bool          complete;
	int           code;
	Buf           content;
	Str           cmd;
	RequestNotify on_complete;
	void*         on_complete_arg;
	List          link;
};

static inline void
request_init(Request* self)
{
	self->type            = REQUEST_EXECUTE;
	self->complete        = false;
	self->code            = 0;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	buf_init(&self->content);
	str_init(&self->cmd);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->link);
}

static inline void
request_free(Request* self)
{
	buf_free_memory(&self->content);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
request_reset(Request* self)
{
	self->type            = REQUEST_EXECUTE;
	self->complete        = false;
	self->code            = 0;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	str_init(&self->cmd);
	buf_reset(&self->content);
}

static inline void
request_complete(Request* self, int code)
{
	mutex_lock(&self->lock);
	self->complete = true;
	self->code     = code;
	if (self->on_complete)
		self->on_complete(self->on_complete_arg);
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline bool
request_wait(Request* self, uint32_t time_ms)
{
	// blocking wait
	if (time_ms == UINT32_MAX)
	{
		mutex_lock(&self->lock);
		while (! self->complete)
			cond_var_wait(&self->cond_var, &self->lock);
		mutex_unlock(&self->lock);
		return true;
	}

	// wait for timeout
	auto complete = false;
	mutex_lock(&self->lock);
	complete = self->complete;
	if (!complete && time_ms != 0)
	{
		cond_var_timedwait(&self->cond_var, &self->lock, time_ms);
		complete = self->complete;
	}
	mutex_unlock(&self->lock);
	return complete;
}
