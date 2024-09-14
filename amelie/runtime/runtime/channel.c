
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>

void
channel_init(Channel* self)
{
	buf_list_init(&self->list);
	condition_init(&self->on_write);
	spinlock_init(&self->lock);
}

void
channel_free(Channel* self)
{
	assert(! self->on_write.attached);
	buf_list_free(&self->list);
	spinlock_free(&self->lock);
}

void
channel_reset(Channel* self)
{
	spinlock_lock(&self->lock);
	buf_list_free(&self->list);
	spinlock_unlock(&self->lock);
}

int
channel_attach_to(Channel* self, Poller* poller)
{
	return condition_attach(&self->on_write, poller);
}

void
channel_attach(Channel* self)
{
	if (self->on_write.attached)
		return;
	int rc = channel_attach_to(self, &am_task->poller);
	if (unlikely(rc == -1))
		error_system();
}

void
channel_detach(Channel* self)
{
	condition_detach(&self->on_write);
}

hot void
channel_write(Channel* self, Buf* buf)
{
	spinlock_lock(&self->lock);
	buf_list_add(&self->list, buf);
	spinlock_unlock(&self->lock);
	condition_signal(&self->on_write);
}

static inline Buf*
channel_pop(Channel* self)
{
	spinlock_lock(&self->lock);
	auto next = buf_list_pop(&self->list);
	spinlock_unlock(&self->lock);
	return next;
}

Buf*
channel_read(Channel* self, int time_ms)
{
	assert(self->on_write.attached);

	// non-blocking
	if (time_ms == 0)
		return channel_pop(self);

	// wait indefinately
	Buf* buf;
	if (time_ms == -1)
	{
		while ((buf = channel_pop(self)) == NULL)
			condition_wait(&self->on_write, -1);
		return buf;
	}

	// timedwait
	buf = channel_pop(self);
	if (buf)
		return buf;
	condition_wait(&self->on_write, time_ms);
	return channel_pop(self);
}
