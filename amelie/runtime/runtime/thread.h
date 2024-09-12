#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Thread Thread;

typedef void* (*ThreadFunction)(void*);

struct Thread
{
	pthread_t      id;
	char           name[64];
	ThreadFunction function;
	void*          arg;
};

static inline void
thread_init(Thread* self)
{
	self->id       = 0;
	self->name[0]  = 0;
	self->function = NULL;
	self->arg      = NULL;
}

static inline bool
thread_created(Thread* self)
{
	return self->id != 0;
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
thread_detach(Thread* self)
{
	int rc;
	rc = pthread_detach(self->id);
	if (rc != 0)
		return -1;
	return 0;
}

static inline int
thread_kill(Thread* self, int sig)
{
	int rc;
	rc = pthread_kill(self->id, sig);
	if (rc != 0)
		return -1;
	return 0;
}

static inline void
thread_set_name(Thread* self, const char* name)
{
	snprintf(self->name, sizeof(self->name), "%s", name);
}

static inline int
thread_update_name(Thread* self)
{
	int rc = pthread_setname_np(self->id, self->name);
	if (rc != 0)
		return -1;
	return 0;
}

static inline void
thread_set_sigmask_default(void)
{
	sigset_t mask;
	sigfillset(&mask);
	pthread_sigmask(SIG_BLOCK, &mask, NULL);
}
