
//
// amelie.
//
// Real-Time SQL Database.
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
	self->list = am_malloc_nothrow(size);
	if (unlikely(self->list == NULL))
		return -1;
	memset(self->list, 0, size);

	// create context
	self->fd = epoll_create(1024);
	if (self->fd == -1)
		return -1;

	return 0;
}

hot int
poller_step(Poller* self, List* read, List* write, int time_ms)
{
	if (self->list_count == 0)
		return 0;

	struct epoll_event* event_list = self->list;
	int count;
	count = epoll_wait(self->fd, event_list, self->list_count, time_ms);
	if (count <= 0)
		return count;

	int total = 0;
	for (int i = 0; i < count; i++)
	{
		struct epoll_event* event = &event_list[i];
		Fd* fd = event->data.ptr;
		if (event->events & EPOLLIN)
		{
			if (read)
				list_append(read, &fd->link);
			fd->on_read = true;
			total++;
		}
		if (event->events & EPOLLOUT ||
		    event->events & EPOLLERR ||
		    event->events & EPOLLHUP)
		{
			if (write)
				list_append(write, &fd->link);
			fd->on_write = true;
			total++;
		}
	}
	return total;
}

int
poller_add(Poller* self, Fd* fd)
{
	int count = self->list_count + 1;
	if (count >= self->list_max)
	{
		int   list_max = self->list_max*  2;
		void* list = am_realloc_nothrow(self->list, list_max * sizeof(struct epoll_event));
		if (unlikely(list == NULL))
			return -1;
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
poller_start_read(Poller* self, Fd* fd)
{
	int mask = fd->mask | EPOLLIN;
	if (mask == fd->mask)
		return 0;
	return poller_mod(self, fd, mask);
}

int
poller_stop_read(Poller* self, Fd* fd)
{
	int mask = fd->mask & ~EPOLLIN;
	if (mask == fd->mask)
		return 0;
	return poller_mod(self, fd, mask);
}

int
poller_start_write(Poller* self, Fd* fd)
{
	int mask = fd->mask | EPOLLOUT;
	if (mask == fd->mask)
		return 0;
	return poller_mod(self, fd, mask);
}

int
poller_stop_write(Poller* self, Fd* fd)
{
	int mask = fd->mask & ~EPOLLOUT;
	if (mask == fd->mask)
		return 0;
	return poller_mod(self, fd, mask);
}
