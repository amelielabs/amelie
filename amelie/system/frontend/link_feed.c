
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
link_subscribe_to(Link* self, Str* user, Str* name)
{
	// find existing feed
	auto feed = feeds_find(&self->feeds, user, name);
	if (feed)
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
	int      flags = 0;
	uint64_t lsn = state_lsn();
	Uuid*    id;
	if (rel->type == REL_SUBSCRIPTION)
	{
		auto sub = sub_of(rel);
		lsn = sub->config->lsn;
		id  = sub->rel_on->id;
		rel = sub->rel_on;

		// include lsn in the data rows
		flags |= CDC_LSN;
	} else {
		id  = rel->id;
	}

	// ensure user can create subscription for that relation
	user_check_permission(self->portal.user, rel, PERM_CREATE_SUBSCRIPTION);

	// (must be under exclusive lock)
	rel->subs++;

	// create feed
	feed = feed_allocate();
	feed_set_user(feed, user);
	if (! str_empty(name))
		feed_set_name(feed, name);
	feed_set_id(feed, id);
	feeds_add(&self->feeds, feed);

	// include target name in the data rows
	if (self->feeds.list_count > 1)
		flags |= CDC_TARGET;
	self->feeds.flags |= flags;

	// open cursor
	cdc_slot_set(&feed->slot, lsn);
	cdc_cursor_open(&feed->cursor, share()->cdc, id, lsn);
}

static inline void
link_subscribe(Link* self, Str* targets)
{
	// target[, ...]
	auto pos = targets->pos;
	auto end = targets->end;

	// take exclusive lock
	portal_lock(&self->portal, LOCK_EXCLUSIVE);

	Str user;
	Str name;
	while (portal_target(&pos, end, &user, &name))
		link_subscribe_to(self, &user, &name);
}

static inline void
link_unsubscribe(Link* self)
{
	if (list_empty(&self->feeds.list))
		return;

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
link_wait(Link* self)
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
	// get min lsn across all feeds
	//
	auto min = feeds_min(&self->feeds);
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
link_feed(Link* self)
{
	// validate and subscribe
	auto portal   = &self->portal;
	auto on_error = error_catch (
		link_subscribe(self, opt_string_of(&portal->endpoint.feed));
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
		// wait for client disconnect or cdc event
		if (link_wait(self))
			break;

		// collect pending cdc events
		buf_reset(buf);
		feeds_collect(&self->feeds, buf);
		if (! buf_empty(buf))
			tcp_write_buf(&self->client->tcp, buf);
	}
}
