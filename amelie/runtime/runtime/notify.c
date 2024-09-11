
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>

void
notify_init(Notify* self)
{
	self->signaled = 0;
	self->poller   = NULL;
	fd_init(&self->fd);
}

int
notify_open(Notify* self, Poller* poller)
{
	self->fd.fd = eventfd(0, EFD_NONBLOCK);
	if (unlikely(self->fd.fd == -1))
		return -1;
	int rc;
	rc = poller_add(poller, &self->fd);
	if (unlikely(rc == -1))
		return -1;
	rc = poller_start_read(poller, &self->fd);
	if (unlikely(rc == -1))
	{
		poller_del(poller, &self->fd);
		return -1;
	}
	self->poller = poller;
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

void
notify_read(Notify* self)
{
	uint64_t id;
	read(self->fd.fd, &id, sizeof(id));
	__sync_lock_release(&self->signaled);
}

void
notify_write(Notify* self)
{
	if (unlikely(self->fd.fd == -1))
		return;
	if (__sync_lock_test_and_set(&self->signaled, 1) == 1)
		return;
	uint64_t id = 1;
	write(self->fd.fd, &id, sizeof(id));
}
