#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Channel Channel;

struct Channel
{
	Mutex   lock;
	CondVar on_write;
	BufList list;
};

static inline void
channel_init(Channel* self)
{
	buf_list_init(&self->list);
	cond_var_init(&self->on_write);
	mutex_init(&self->lock);
}

static inline void
channel_free(Channel* self)
{
	buf_list_free(&self->list);
	cond_var_free(&self->on_write);
	mutex_free(&self->lock);
}

static inline void
channel_reset(Channel* self)
{
	mutex_lock(&self->lock);
	buf_list_free(&self->list);
	mutex_unlock(&self->lock);
}

hot static inline void
channel_write(Channel* self, Buf* buf)
{
	mutex_lock(&self->lock);
	buf_list_add(&self->list, buf);
	cond_var_signal(&self->on_write);
	mutex_unlock(&self->lock);
}

static inline Buf*
channel_read(Channel* self)
{
	mutex_lock(&self->lock);
	Buf* buf = NULL;
	for (;;)
	{
		buf = buf_list_pop(&self->list);
		if (buf)
			break;
		cond_var_wait(&self->on_write, &self->lock);
	}
	mutex_unlock(&self->lock);
	return buf;
}
