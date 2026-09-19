
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

typedef struct Feed Feed;

struct Feed
{
	Client* client;
	Portal* portal;
	Streams streams;
};

static inline void
feed_init(Feed* self, Client* client, Portal* portal)
{
	self->client = client;
	self->portal = portal;
	streams_init(&self->streams, share()->cdc);
}

static inline void
feed_free(Feed* self)
{
	streams_free(&self->streams);
}

static inline void
feed_subscribe_to(Feed* self, Str* user, Str* name)
{
	// find existing stream
	auto stream = streams_find(&self->streams, user, name);
	if (stream)
		error("relation '{str}': is redefined", name);

	// find user or relation
	Rel* rel = NULL;
	if (str_empty(name))
	{
		auto ref = catalog_find_user(&share()->db->catalog, user, false);
		if (! ref)
			error("user '{str}': not found", user);
		rel = &ref->rel;
	} else
	{
		rel = catalog_find(&share()->db->catalog, REL_UNDEF, user, name, false);
		if (! rel)
			error("relation '{str}.{str}': does not exists", user, name);

		if (rel->type != REL_TABLE &&
		    rel->type != REL_CLONE &&
		    rel->type != REL_TOPIC &&
		    rel->type != REL_SUBSCRIPTION)
			error("relation '{str}.{str}': is not supported for streaming", user, name);
	}

	// use subscription relation
	uint64_t lsn = state_lsn();
	Uuid*    id;
	if (rel->type == REL_SUBSCRIPTION)
	{
		auto sub = sub_of(rel);
		lsn = sub->config->lsn;
		id  = sub->rel_on->id;
		rel = sub->rel_on;
	} else {
		id  = rel->id;
	}

	// ensure user can create subscription for that relation
	user_check_permission(self->portal->user, rel, PERM_CREATE_SUBSCRIPTION);

	// (must be under exclusive lock)
	rel->subs++;

	// create stream
	stream = stream_allocate();
	stream_set_user(stream, user);
	if (! str_empty(name))
		stream_set_name(stream, name);
	stream_set_id(stream, id);
	streams_add(&self->streams, stream);

	// open cursor
	cdc_slot_set(&stream->slot, lsn);
	cdc_cursor_open(&stream->cursor, share()->cdc, id, lsn);
}

static inline void
feed_subscribe(Feed* self, Str* targets)
{
	// target[, ...]
	auto pos = targets->pos;
	auto end = targets->end;

	// take exclusive lock
	portal_lock(self->portal, LOCK_EXCLUSIVE);

	Str user;
	Str name;
	while (portal_target(&pos, end, &user, &name))
		feed_subscribe_to(self, &user, &name);
}

static inline void
feed_unsubscribe(Feed* self)
{
	if (list_empty(&self->streams.list))
		return;

	// take exclusive catalog lock
	auto lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);
	defer(unlock, lock);

	auto catalog = &share()->db->catalog;
	list_foreach(&self->streams.list)
	{
		auto feed = list_at(Stream, link);
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
}

static inline void
feed_begin(Feed* self)
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
feed_wait(Feed* self)
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

	// prepare cdc sub
	//
	// get min lsn across all streams
	//
	auto min = streams_min(&self->streams);
	CdcSub sub;
	cdc_sub_init(&sub, &event_sub, min);
	cdc_subscribe(share()->cdc, &sub);

	// wait
	auto on_error = error_catch
	(
		poll_read_start(&self->client->tcp.fd, &event_client);
		event_wait(&event, -1);
	);
	poll_read_stop(&self->client->tcp.fd);
	cdc_unsubscribe(share()->cdc, &sub);

	if (unlikely(on_error))
		rethrow();

	return event_client.signal;
}

void
frontend_feed(Frontend* self, Client* client, Portal* portal, Str* targets)
{
	// note: portal keeps shared lock
	unused(self);

	Feed feed;
	feed_init(&feed, client, portal);
	defer(feed_free, &feed);

	// validate and subscribe
	auto on_error = error_catch
	(
		feed_subscribe(&feed, targets);
	);
	portal_unlock(portal);

	auto buf = portal->output.buf;
	if (on_error)
	{
		feed_unsubscribe(&feed);

		buf_reset(buf);
		output_error(&portal->output, &am_self()->error);
		client_400(client, buf);
		return;
	}

	defer(feed_unsubscribe, &feed);

	// SSE
	feed_begin(&feed);

	for (;;)
	{
		// wait for client disconnect or cdc event
		if (feed_wait(&feed))
			break;

		// collect pending cdc events
		buf_reset(buf);
		streams_collect(&feed.streams, buf);
		if (! buf_empty(buf))
			tcp_write_buf(&client->tcp, buf);
	}
}
