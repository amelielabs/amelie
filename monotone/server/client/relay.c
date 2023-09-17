
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>

hot static bool
relay_from_client(Connection* conn, Channel* chan)
{
	bool close = false;
	while (! close)
	{
		auto buf = channel_read(chan, 0);
		if (buf == NULL)
			break;
		auto msg = msg_of(buf);
		if (unlikely(msg->id == MSG_CONNECT))
		{
			buf_free(buf);
			continue;
		}
		close = msg->id == MSG_DISCONNECT;
		if (unlikely(close))
		{
			buf_free(buf);
			break;
		}
		tcp_send_add(&conn->tcp, buf);
	}
	tcp_flush(&conn->tcp);
	return close;
}

hot static bool
relay_from_connection(Connection* conn, Channel* chan)
{
	// todo: send whole list
	for (;;)
	{
		auto buf = tcp_recv_try(&conn->tcp);
		if (buf == NULL)
			break;
		if (chan)
			channel_write(chan, buf);
		else
			buf_free(buf);
	}
	return tcp_eof(&conn->tcp);
}

hot bool
relay(Connection* conn, Channel* read, Channel* write)
{
	Event on_io;
	event_init(&on_io);

	// on connection read
	Event on_read;
	event_init(&on_read);
	event_set_parent(&on_read, &on_io);
	tcp_read_start(&conn->tcp, &on_read);

	// on write to the read ipc
	Event *on_write;
	on_write = &read->on_write.event;
	event_set_parent(on_write, &on_io);
	if (event_pending(on_write))
		event_signal(&on_io);

	bool on_disconnect = false;
	Exception e;
	if (try(&e))
	{
		for (;;)
		{
			event_wait(&on_io, -1);

			// read from channel and send to connection
			if (event_pending(on_write))
			{
				on_disconnect = relay_from_client(conn, read);
				if (on_disconnect)
					break;
			}

			// read from connection and write to channel
			if (event_pending(&on_read))
				if (relay_from_connection(conn, write))
					break;
		}
	}

	if (catch(&e))
	{ }

	event_set_parent(on_write, NULL);
	tcp_read_stop(&conn->tcp);
	return on_disconnect;
}
