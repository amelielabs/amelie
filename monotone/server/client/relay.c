
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
relay_from_channel(Client* client, Channel* chan)
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
		tcp_send_add(&client->tcp, buf);
	}
	tcp_flush(&client->tcp);
	return close;
}

hot static bool
relay_from_client(Client* client, Channel* chan)
{
	// todo: send whole list
	for (;;)
	{
		auto buf = tcp_recv_try(&client->tcp);
		if (buf == NULL)
			break;
		if (chan)
			channel_write(chan, buf);
		else
			buf_free(buf);
	}
	return tcp_eof(&client->tcp);
}

hot bool
relay(Client* client, Channel* read, Channel* write)
{
	Event on_io;
	event_init(&on_io);

	// on client read
	Event on_read;
	event_init(&on_read);
	event_set_parent(&on_read, &on_io);
	tcp_read_start(&client->tcp, &on_read);

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

			// read from channel and send to client
			if (event_pending(on_write))
			{
				on_disconnect = relay_from_channel(client, read);
				if (on_disconnect)
					break;
			}

			// read from client and write to channel
			if (event_pending(&on_read))
				if (relay_from_client(client, write))
					break;
		}
	}

	if (catch(&e))
	{ }

	event_set_parent(on_write, NULL);
	tcp_read_stop(&client->tcp);
	return on_disconnect;
}
