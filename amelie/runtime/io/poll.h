#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline void
poll_read(Fd* fd, int time_ms)
{
	cancellation_point();
	auto poller = &am_self->poller;
	fd_reset(fd);
	while (! fd->on_read)
	{
		auto rc = poller_step(poller, NULL, NULL, time_ms);
		cancellation_point();
		if (unlikely(rc == -1 && errno != EINTR))
			error_system();
	}
}

hot static inline void
poll_write(Fd* fd, int time_ms)
{
	cancellation_point();
	auto poller = &am_self->poller;
	fd_reset(fd);
	auto rc = poller_start_write(poller, fd);
	if (unlikely(rc == -1))
		error_system();
	while (! fd->on_write)
	{
		rc = poller_step(poller, NULL, NULL, time_ms);
		cancellation_point();
		if (unlikely(rc == -1 && errno != EINTR))
			error_system();
	}
	poller_stop_write(poller, fd);
}
