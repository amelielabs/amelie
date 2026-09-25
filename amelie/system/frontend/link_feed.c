
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>

static inline void
link_subscribe(Link* self, StreamCursor* cursor)
{
	// find channel
	auto api = self->api;
	auto rel = catalog_find(&share()->db->catalog, REL_UNDEF, &api->rel_user, &api->rel, false);
	if (! rel)
		error("relation '{str}.{str}': does not exists",
		      &api->rel_user, &api->rel);

	if (rel->type != REL_CHANNEL)
		error("relation '{str}.{str}': is not a channel",
		      &api->rel_user, &api->rel);

	// take stream reference
	self->stream = channel_of(rel)->stream;
	stream_ref(self->stream);

	// open cursor
	auto id = opt_int_of(&self->client->endpoint->id);
	stream_cursor_open(cursor, self->stream, id);
}

static inline void
link_unsubscribe(Link* self)
{
	if (self->stream)
	{
		stream_unref(self->stream);
		self->stream = NULL;
	}
}

static inline void
link_feed_begin(Link* self)
{
	auto client = self->client;
	auto reply = &client->reply;
	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6, 0);
	buf_write(buf, "Cache-Control: no-cache\r\n", 25);
	buf_write(buf, "Connection: keep-alive\r\n", 24);
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);
}

hot static inline bool
link_wait(Link* self, StreamCursor* cursor)
{
	// parent
	Event event;
	event_init(&event);
	event_attach(&event);

	// prepare client event
	Event event_client;
	event_init(&event_client);
	event_set_parent(&event_client, &event);
	event_attach(&event_client);

	// prepare sub event
	Event event_sub;
	event_init(&event_sub);
	event_set_parent(&event_sub, &event);
	event_attach(&event_sub);

	// prepare stream subscription
	StreamSub sub;
	stream_sub_init(&sub, &event_sub, cursor->id);
	stream_subscribe(cursor->stream, &sub);

	// wait
	auto on_error = error_catch
	(
		poll_read_start(&self->client->tcp.fd, &event_client);
		event_wait(&event, -1);
	);
	poll_read_stop(&self->client->tcp.fd);
	stream_unsubscribe(cursor->stream, &sub);

	if (unlikely(on_error))
		rethrow();

	// client disconnect on stream shutdown
	return event_client.signal || sub.shutdown;
}

void
link_feed(Link* self)
{
	StreamCursor cursor;
	stream_cursor_init(&cursor);

	// validate and subscribe
	auto portal   = &self->portal;
	auto on_error = error_catch
	(
		link_subscribe(self, &cursor);
	);

	// release catalog lock (stream is holding a reference)
	portal_unlock(portal);

	auto buf = portal->output.buf;
	if (on_error)
	{
		link_unsubscribe(self);

		buf_reset(buf);
		output_error(&portal->output, &am_self()->error);
		client_400(self->client, buf);
		return;
	}

	defer(link_unsubscribe, self);

	// SSE
	link_feed_begin(self);
	for (;;)
	{
		// collect events
		buf_reset(buf);
		stream_cursor_collect(&cursor, buf);
		if (! buf_empty(buf))
		{
			tcp_write_buf(&self->client->tcp, buf);
			continue;
		}

		// wait for first event, eof or channel drop
		if (link_wait(self, &cursor))
			break;
	}
}
