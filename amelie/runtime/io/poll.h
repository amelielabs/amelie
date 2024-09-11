#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline void
poll_read(Fd* fd, int time_ms)
{
	auto poller = &am_self->poller;
	fd_reset(fd);
	auto rc = poller_start_read(poller, fd);
	if (unlikely(rc == -1))
		error_system();
	while (! fd->on_read)
	{
		rc = poller_step(poller, NULL, NULL, time_ms);
		if (unlikely(rc == -1))
			error_system();
	}
	poller_stop_read(poller, fd);
}

hot static inline void
poll_write(Fd* fd, int time_ms)
{
	auto poller = &am_self->poller;
	fd_reset(fd);
	auto rc = poller_start_write(poller, fd);
	if (unlikely(rc == -1))
		error_system();
	while (! fd->on_write)
	{
		rc = poller_step(poller, NULL, NULL, time_ms);
		if (unlikely(rc == -1))
			error_system();
	}
	poller_stop_write(poller, fd);
}
