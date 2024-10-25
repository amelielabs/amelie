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

typedef struct CondVar CondVar;

struct CondVar
{
	pthread_cond_t var;
};

static inline void
cond_var_init(CondVar* self)
{
	pthread_cond_init(&self->var, NULL);
}

static inline void
cond_var_free(CondVar* self)
{
	pthread_cond_destroy(&self->var);
}

static inline void
cond_var_signal(CondVar* self)
{
	pthread_cond_signal(&self->var);
}

static inline void
cond_var_broadcast(CondVar* self)
{
	pthread_cond_broadcast(&self->var);
}

static inline void
cond_var_wait(CondVar* self, Mutex* mutex)
{
	pthread_cond_wait(&self->var, &mutex->lock);
}

static inline void
cond_var_timedwait(CondVar* self, Mutex* mutex, int time_ms)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	struct timespec ts;
    ts.tv_sec  = tv.tv_sec + (time_ms / 1000);
    ts.tv_nsec = (tv.tv_usec * 1000) + (time_ms % 1000) * 1000000;
	pthread_cond_timedwait(&self->var, &mutex->lock, &ts);
}
