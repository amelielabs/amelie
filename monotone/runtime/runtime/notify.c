
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>

static void
notify_on_read(Fd* fd)
{
	Notify* self = fd->on_read_arg;
	uint64_t id;
	read(fd->fd, &id, sizeof(id));
	__sync_lock_release(&self->signaled);
	if (self->on_notify)
		self->on_notify(self->arg);
}

void
notify_init(Notify* self)
{
	memset(&self->fd, 0, sizeof(self->fd));
	self->fd.fd     = -1;
	self->signaled  = 0;
	self->on_notify = NULL;
	self->arg       = NULL;
	self->poller    = NULL;
}

int
notify_open(Notify* self, Poller* poller, NotifyFunction callback, void* arg)
{
	self->fd.fd = eventfd(0, EFD_NONBLOCK);
	if (unlikely(self->fd.fd == -1))
		return -1;
	int rc;
	rc = poller_add(poller, &self->fd);
	if (unlikely(rc == -1))
		return -1;
	rc = poller_read(poller, &self->fd, notify_on_read, self);
	if (unlikely(rc == -1)) {
		poller_del(poller, &self->fd);
		return -1;
	}
	self->on_notify = callback;
	self->arg       = arg;
	self->poller    = poller;
	return 0;
}

void
notify_close(Notify* self)
{
	if (self->fd.fd == -1)
		return;
	poller_del(self->poller, &self->fd);
	close(self->fd.fd);
	self->fd.fd = -1;
}

static inline void
notify_send(int fd)
{
	if (unlikely(fd == -1))
		return;
	uint64_t id = 1;
	write(fd, &id, sizeof(id));
}

void
notify_signal(Notify* self)
{
	if (unlikely(self->fd.fd == -1))
		return;
	if (__sync_lock_test_and_set(&self->signaled, 1) == 1)
		return;
	notify_send(self->fd.fd);
}
