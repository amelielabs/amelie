
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

typedef struct Follower Follower;

struct Follower
{
	Websocket ws;
	Request*  req;
	Api*      api;
	Query     query;
	void*     session;
	CdcSub    cdc_sub;
	Feeds     feeds;
	Str       protocol;
	Frontend* fe;
};

static inline void
follower_init(Follower* self, Frontend* fe, Client* client,
              Request*  req,
              Api*      api, void* session)
{
	self->req     = req;
	self->api     = api;
	self->session = session;
	self->fe      = fe;
	str_set(&self->protocol, "amelie-v1", 9);
	websocket_init(&self->ws);
	websocket_set(&self->ws, &self->protocol, client, false);
	feeds_init(&self->feeds, share()->cdc);
	query_init(&self->query);
}

static inline void
follower_free(Follower* self)
{
	// unreference relations
	list_foreach(&self->feeds.list)
	{
		auto feed = list_at(Feed, link);
		auto rel_match = catalog_find_by(&share()->db->catalog, REL_UNDEF, &feed->id, false);
		if (rel_match)
		{
			rel_match->subs--;
			assert(rel_match->subs >= 0);
		}
	}
	feeds_free(&self->feeds);
	websocket_free(&self->ws);
}

static inline void
follower_reset(Follower* self)
{
	// unlock catalog
	auto req = self->req;
	request_reset(req, false);
	output_set(&req->output, &req->endpoint, &output_jsonrpc, NULL);
	api_reset(self->api);
	query_init(&self->query);
}

static inline void
follower_accept(Follower* self)
{
	// websocket handshake
	websocket_accept(&self->ws);
}

static void
follower_follow(Follower* self)
{
	// PERM_CREATE_SUBSCRIPTION
	auto api = self->api;
	auto req = self->req;
	user_check(req->user, PERM_CREATE_SUBSCRIPTION);

	// find existing feed
	Str* user;
	if (str_empty(&api->rel_user))
		user = &req->user->config->name;
	else
		user = &api->rel_user;
	auto feed = feeds_find(&self->feeds, user, &api->rel);
	if (feed)
		error("feed '{str}': already exists", &api->rel);

	// find relation
	auto rel = catalog_find(&share()->db->catalog, REL_UNDEF, user, &api->rel, false);
	if (! rel)
		error("feed '{str}': relation not found", &api->rel);

	if (rel->type != REL_TABLE &&
	    rel->type != REL_CLONE &&
	    rel->type != REL_TOPIC &&
	    rel->type != REL_SUBSCRIPTION)
		error("feed '{str}': {s} cannot be used here", &api->rel,
		      rel_type_of(rel->type));

	uint64_t lsn    = state_lsn() + 1;
	uint32_t lsn_op = 0;
	Uuid*    id;

	auto rel_on = rel;
	if (rel->type == REL_SUBSCRIPTION)
	{
		auto sub = sub_of(rel);
		lsn    = sub->config->lsn;
		lsn_op = sub->config->lsn_op + 1;
		id     = sub->rel_on->id;
		rel_on = sub->rel_on;
	} else {
		id     = rel->id;
	}

	// ensure user can create subscription for that relation
	user_check_permission(req->user, rel_on, PERM_CREATE_SUBSCRIPTION);
	rel_on->subs++;

	// create and register feed
	feed = feed_allocate();
	feed_set_user(feed, user);
	feed_set_name(feed, &api->rel);
	feed_set_id(feed, id);
	feeds_add(&self->feeds, feed);

	// open cursor
	cdc_slot_set(&feed->slot, lsn, lsn_op);
	cdc_cursor_open(&feed->cursor, share()->cdc, id, lsn, lsn_op);
}

static void
follower_unfollow(Follower* self)
{
	auto api = self->api;

	// find existing feed
	Str* user;
	if (str_empty(&api->rel_user))
		user = &self->req->user->config->name;
	else
		user = &api->rel_user;
	auto feed = feeds_find(&self->feeds, user, &api->rel);
	if (! feed)
		error("feed '{str}': does not exists", &api->rel);

	// unref and remove
	auto rel_match = catalog_find_by(&share()->db->catalog, REL_UNDEF, &feed->id, false);
	if (rel_match)
	{
		rel_match->subs--;
		assert(rel_match->subs >= 0);
	}

	feeds_remove(&self->feeds, feed);
	feed_free(feed);
}

hot static void
follower_execute(Follower* self, Str* content)
{
	// set time and random seed
	auto req = self->req;
	request_prepare(req);

	// parse
	auto query = &self->query;
	auto api = self->api;
	auto on_error = error_catch
	(
		// auth (take catalog lock)
		request_auth(req, &self->fe->auth);

		// parse
		if (! api_parse(api, content, query, true))
			rethrow();

		if (api->type == API_FOLLOW)
			follower_follow(self);
		else
		if (api->type == API_UNFOLLOW)
			follower_unfollow(self);
	);
	if (on_error)
	{
		output_error(&req->output, &am_self()->error);
		return;
	}

	// execute by session, unle
	if (query->type != QUERY_UNDEF)
		self->fe->iface->session_execute(self->session, req, query);

	// handle empty result
	if (buf_empty(req->output.buf))
		output_none(&req->output);
}

hot static void
follower_collect(Follower* self)
{
	auto buf = self->req->output.buf;
	feeds_collect(&self->feeds, buf);
}

hot static inline bool
follower_wait(Follower* self)
{
	// if no feeds, go straight to io wait
	if (feeds_empty(&self->feeds))
		return true;

	// client has pending data
	auto ws = &self->ws;
	if (readahead_pending(&ws->client->readahead))
		return true;

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
	uint64_t min = feeds_min(&self->feeds);
	CdcSub sub;
	cdc_sub_init(&sub, &event_sub, min);
	cdc_subscribe(share()->cdc, &sub);

	// wait
	auto on_error = error_catch
	(
		poll_read_start(&ws->client->tcp.fd, &event_client);
		event_wait(&event, -1);
	);
	poll_read_stop(&ws->client->tcp.fd);
	cdc_unsubscribe(share()->cdc, &sub);

	if (unlikely(on_error))
		rethrow();

	return event_client.signal;
}

hot static inline bool
follower_recv(Follower* self, Str* content)
{
	auto ws = &self->ws;
	if (! websocket_recv(ws, NULL, 0))
		return false;
	auto buf = &ws->client->request.content;
	buf_reset(buf);
	websocket_recv_data(ws, buf);
	buf_str(buf, content);
	return true;
}

static inline void
follower_send(Follower* self)
{
	auto buf = self->req->output.buf;
	if (buf_empty(buf))
		return;
	struct iovec iov;
	iov_set_buf(&iov, buf);
	websocket_send(&self->ws, WS_TEXT, &iov, 1, 0);
}

hot void
frontend_follower(Frontend* self, Client* client,
                  Request*  req,
                  Api*      api, void* session)
{
	Follower follower;
	follower_init(&follower, self, client, req, api, session);
	defer(follower_free, &follower);
	follower_accept(&follower);

	for (;;)
	{
		// reset and unlock catalog
		follower_reset(&follower);

		// wait for client io or cdc event
		if (follower_wait(&follower))
		{
			// recv jsonrpc request
			Str content;
			if (! follower_recv(&follower, &content))
				break;
			// parse and execute
			follower_execute(&follower, &content);
		}

		// collect pending cdc events
		follower_collect(&follower);

		// batch send
		follower_send(&follower);
	}
}
