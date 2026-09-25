
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

	// open cursor
	auto channel = channel_of(rel);
	stream_cursor_open(cursor, &channel->stream, 0);
}

static inline void
link_unsubscribe(Link* self)
{
	(void)self;
#if 0
	// take exclusive catalog lock
	auto lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	defer(unlock, lock);

	auto catalog = &share()->db->catalog;
	list_foreach(&self->feeds.list)
	{
		auto feed = list_at(Feed, link);
		Rels* rels;
		if (str_empty(&feed->name))
			rels = &catalog->users;
		else
			rels = &catalog->rels;
		auto rel = rels_find_by(rels, REL_UNDEF, &feed->id, false);
		if (! rel)
			continue;
		rel->subs--;
		assert(rel->subs >= 0);
	}
#endif
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
	stream_sub_init(&sub, &event_sub, cursor->id + 1);
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

	return event_client.signal;
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
		// wait for client disconnect or first channel event
		if (link_wait(self, &cursor))
			break;

		// collect events
		buf_reset(buf);
		stream_cursor_collect(&cursor, buf);
		if (! buf_empty(buf))
			tcp_write_buf(&self->client->tcp, buf);
	}
}
