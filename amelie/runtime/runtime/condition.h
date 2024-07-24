#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Condition Condition;

struct Condition
{
	Notify notify;
	Event  event;
	bool   attached;
	List   link;
};

static inline void
condition_init(Condition* self)
{
	self->attached = false;
	event_init(&self->event);
	notify_init(&self->notify);
}

static inline void
condition_on_notify(void* arg)
{
	Condition* self = arg;
	event_signal(&self->event);
}

static inline int
condition_attach(Condition* self, Poller* poller)
{
	if (self->attached)
		return 0;
	int rc;
	rc = notify_open(&self->notify, poller, condition_on_notify, self);
	if (unlikely(rc == -1))
		return -1;
	self->attached = true;
	return 0;
}

static inline void
condition_detach(Condition* self)
{
	if (! self->attached)
		return;
	notify_close(&self->notify);
	self->attached = false;
}

static inline void
condition_signal(Condition* self)
{
	notify_signal(&self->notify);
}
