#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct ThreadStatus ThreadStatus;

struct ThreadStatus
{
	Mutex   lock;
	CondVar cond_var;
	bool    signal; 
	int     status;
};

static inline void
thread_status_init(ThreadStatus* self)
{
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	self->signal = false;
	self->status = 0;
}

static inline void
thread_status_free(ThreadStatus* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
thread_status_reset(ThreadStatus* self)
{
	self->signal = false;
	self->status = 0;
}

static inline void
thread_status_set(ThreadStatus* self, int value)
{
	mutex_lock(&self->lock);
	self->signal = true;
	self->status = value;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

static inline int
thread_status_wait(ThreadStatus* self)
{
	mutex_lock(&self->lock);
	while (! self->signal)
		cond_var_wait(&self->cond_var, &self->lock);
	int value;
	value = self->status;
	mutex_unlock(&self->lock);
	return value;
}
