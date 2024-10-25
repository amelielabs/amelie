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

typedef struct Mutex Mutex;

struct Mutex
{
	pthread_mutex_t lock;
};

static inline void
mutex_init(Mutex* self)
{
	pthread_mutex_init(&self->lock, NULL);
}

static inline void
mutex_free(Mutex* self)
{
	pthread_mutex_destroy(&self->lock);
}

static inline void
mutex_lock(Mutex* self)
{
	pthread_mutex_lock(&self->lock);
}

static inline void
mutex_unlock(Mutex* self)
{
	pthread_mutex_unlock(&self->lock);
}
