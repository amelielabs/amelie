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

typedef struct Cond Cond;

struct Cond
{
	Mutex   lock;
	CondVar cond_var;
	bool    signal; 
	int     status;
};

static inline void
cond_init(Cond* self)
{
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	self->signal = false;
	self->status = 0;
}

static inline void
cond_free(Cond* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
cond_reset(Cond* self)
{
	self->signal = false;
	self->status = 0;
}

static inline void
cond_signal(Cond* self, int value)
{
	mutex_lock(&self->lock);
	self->signal = true;
	self->status = value;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline int
cond_wait(Cond* self)
{
	mutex_lock(&self->lock);
	while (! self->signal)
		cond_var_wait(&self->cond_var, &self->lock);
	int value;
	value = self->status;
	mutex_unlock(&self->lock);
	return value;
}
