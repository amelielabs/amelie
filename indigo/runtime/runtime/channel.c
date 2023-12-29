
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>

void
channel_init(Channel* self)
{
	buf_pool_init(&self->pool);
	condition_init(&self->on_write);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
}

void
channel_free(Channel* self)
{
	assert(! self->on_write.attached);
	buf_pool_reset(&self->pool);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

int
channel_attach_to(Channel* self, Poller* poller)
{
	return condition_attach(&self->on_write, poller);
}

void
channel_attach(Channel* self)
{
	int rc = channel_attach_to(self, &mn_task->poller);
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
	mutex_lock(&self->lock);

	buf_pool_add(&self->pool, buf);
	if (self->on_write.attached)
		condition_signal(&self->on_write);

	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}

hot static inline Buf*
read_and_pin(Channel* self)
{
	mutex_lock(&self->lock);
	auto next = buf_pool_pop(&self->pool, &mn_self()->buf_pool);
	mutex_unlock(&self->lock);
	return next;
}

Buf*
channel_read(Channel* self, int time_ms)
{
	// attached to a task
	Buf* buf;
	if (self->on_write.attached)
	{
		// non-blocking
		if (time_ms == 0)
			return read_and_pin(self);

		// wait indefinately
		if (time_ms == -1)
		{
			while ((buf = read_and_pin(self)) == NULL)
				condition_wait(&self->on_write, -1);
			return buf;
		}

		// timedwait
		buf = read_and_pin(self);
		if (buf)
			return buf;
		condition_wait(&self->on_write, time_ms);
		return read_and_pin(self);
	}

	// channel is not attached to the runtime

	// non-blocking
	if (time_ms == 0)
	{
		mutex_lock(&self->lock);
		buf = buf_pool_pop(&self->pool, NULL);
		mutex_unlock(&self->lock);
		return buf;
	}

	// wait indefinately
	if (time_ms == -1)
	{
		mutex_lock(&self->lock);
		for (;;)
		{
			buf = buf_pool_pop(&self->pool, NULL);
			if (buf)
				break;
			cond_var_wait(&self->cond_var, &self->lock);
		}
		mutex_unlock(&self->lock);
		return buf;
	}

	// timedwait
	mutex_lock(&self->lock);
	buf = buf_pool_pop(&self->pool, NULL);
	if (buf == NULL)
	{
		cond_var_timedwait(&self->cond_var, &self->lock, time_ms);
		buf = buf_pool_pop(&self->pool, NULL);
	}
	mutex_unlock(&self->lock);
	return buf;
}
