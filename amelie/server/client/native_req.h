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

typedef struct NativeReq NativeReq;

enum
{
	NATIVE_CONNECT,
	NATIVE_DISCONNECT,
	NATIVE_EXECUTE
};

typedef void (*NativeNotify)(void*);

struct NativeReq
{
	Mutex        lock;
	CondVar      cond_var;
	int          type;
	bool         complete;
	int          code;
	Buf          output;
	Str          cmd;
	NativeNotify on_complete;
	void*        on_complete_arg;
	List         link;
};

static inline void
native_req_init(NativeReq* self)
{
	self->type            = NATIVE_EXECUTE;
	self->complete        = false;
	self->code            = 0;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	buf_init(&self->output);
	str_init(&self->cmd);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->link);
}

static inline void
native_req_free(NativeReq* self)
{
	buf_free_memory(&self->output);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
native_req_reset(NativeReq* self)
{
	self->type            = NATIVE_EXECUTE;
	self->complete        = false;
	self->code            = 0;
	self->on_complete     = NULL;
	self->on_complete_arg = NULL;
	str_init(&self->cmd);
	buf_reset(&self->output);
}

static inline void
native_req_complete(NativeReq* self, int code)
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
native_req_wait(NativeReq* self, uint32_t time_ms)
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
