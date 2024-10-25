
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

#include <amelie_runtime.h>

void
poller_init(Poller* self)
{
	self->fd         = -1;
	self->list       = NULL;
	self->list_count = 0;
	self->list_max   = 0;
}

void
poller_free(Poller* self)
{
	if (self->fd != -1)
	{
		close(self->fd);
		self->fd = -1;
	}
	if (self->list)
		am_free(self->list);
}

int
poller_create(Poller* self)
{
	self->list_max = 1024;

	// prepare event list
	int size = sizeof(struct epoll_event) * 1024;
	self->list = am_malloc(size);
	memset(self->list, 0, size);

	// create context
	self->fd = epoll_create(1024 /* ignored */);
	if (self->fd == -1)
		return -1;

	return 0;
}

hot int
poller_step(Poller* self, int time_ms)
{
	if (self->list_count == 0)
		return 0;

	struct epoll_event* event_list = self->list;
	int count;
	count = epoll_wait(self->fd, event_list, self->list_count, time_ms);
	if (count <= 0)
		return 0;

	int i = 0;
	while (i < count)
	{
		struct epoll_event* event = &event_list[i];
		Fd* fd = event->data.ptr;
		if (fd->on_read)
		{
			if (event->events & EPOLLIN)
				fd->on_read(fd);
		}
		if (fd->on_write)
		{
			if (event->events & EPOLLOUT ||
				event->events & EPOLLERR ||
				event->events & EPOLLHUP)
			{
				fd->on_write(fd);
			}
		}
		i++;
	}

	return count;
}

int
poller_add(Poller* self, Fd* fd)
{
	int count = self->list_count + 1;
	if (count >= self->list_max)
	{
		int   list_max = self->list_max*  2;
		void* list = am_realloc(self->list, list_max * sizeof(struct epoll_event));
		self->list = list;
		self->list_max = list_max;
	}

	struct epoll_event event;
	event.events = 0;
	event.data.ptr = fd;
	int rc;
	rc = epoll_ctl(self->fd, EPOLL_CTL_ADD, fd->fd, &event);
	if (unlikely(rc == -1))
		return -1;
	self->list_count++;
	return 0;
}

int
poller_del(Poller* self, Fd* fd)
{
	struct epoll_event event;
	memset(&event, 0, sizeof(event));
	int rc;
	rc = epoll_ctl(self->fd, EPOLL_CTL_DEL, fd->fd, &event);
	if (unlikely(rc == -1))
		return -1;
	self->list_count--;
	assert(self->list_count >= 0);
	return 0;
}

static inline int
poller_mod(Poller* self, Fd* fd, int mask)
{
	fd->mask = mask;
	struct epoll_event event;
	event.events = mask;
	event.data.ptr = fd;
	return epoll_ctl(self->fd, EPOLL_CTL_MOD, fd->fd, &event);
}

int
poller_read(Poller*    self,
            Fd*        fd,
            FdFunction on_read,
            void*      arg)
{
	int mask = fd->mask;
	if (on_read)
		mask |= EPOLLIN;
	else
		mask &= ~EPOLLIN;
	if (mask == fd->mask)
		return 0;
	fd->on_read     = on_read;
	fd->on_read_arg = arg;
	return poller_mod(self, fd, mask);
}

int
poller_write(Poller*    self,
             Fd*        fd,
             FdFunction on_write,
             void*      arg)
{
	int mask = fd->mask;
	if (on_write)
		mask |= EPOLLOUT;
	else
		mask &= ~EPOLLOUT;
	if (mask == fd->mask)
		return 0;
	fd->on_write     = on_write;
	fd->on_write_arg = arg;
	return poller_mod(self, fd, mask);
}
