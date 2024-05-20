#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Thread Thread;

typedef void* (*ThreadFunction)(void*);

struct Thread
{
	pthread_t      id;
	ThreadFunction function;
	void*          arg;
};

static inline void
thread_init(Thread* self)
{
	self->id       = 0;
	self->function = NULL;
	self->arg      = NULL;
}

static inline int
thread_create(Thread* self, ThreadFunction function, void* arg)
{
	self->function = function;
	self->arg = arg;
	int rc;
	rc = pthread_create(&self->id, NULL, function, self);
	if (rc != 0)
		return -1;
	return 0;
}

static inline int
thread_join(Thread* self)
{
	int rc;
	rc = pthread_join(self->id, NULL);
	if (rc != 0)
		return -1;
	self->id = 0;
	return 0;
}

static inline int
thread_set_name(Thread* self, const char* name)
{
	int rc;
	rc = pthread_setname_np(self->id, name);
	return rc;
}

static inline void
thread_set_sigmask_default(void)
{
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGPIPE);
	pthread_sigmask(SIG_BLOCK, &mask, NULL);
}
