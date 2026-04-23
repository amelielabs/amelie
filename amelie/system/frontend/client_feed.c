
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
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>

typedef struct Feed Feed;

struct Feed
{
	Websocket    ws;
	Request*     req;
	void*        session;
	CdcSub       cdc_sub;
	SubscribeMgr subscribe_mgr;
	Str          protocol;
	Frontend*    fe;
};

static inline void
feed_init(Feed* self, Frontend* fe, Client* client, Request* req, void* session)
{
	self->req     = req;
	self->session = session;
	self->fe      = fe;
	str_set(&self->protocol, "amelie-v1", 9);
	websocket_init(&self->ws);
	websocket_set(&self->ws, &self->protocol, client, false);
	subscribe_mgr_init(&self->subscribe_mgr, share()->cdc);
}

static inline void
feed_free(Feed* self)
{
	websocket_free(&self->ws);
	subscribe_mgr_free(&self->subscribe_mgr);
}

static inline void
feed_reset(Feed* self)
{
	// unlock catalog
	auto req = self->req;
	request_reset(req, false);
	output_set(&req->output, &req->endpoint);
}

static inline void
feed_accept(Feed* self)
{
	// websocket handshake
	websocket_accept(&self->ws);
}

hot static void
feed_subscribe(Feed* self)
{
	// todo: check GRANT_SUBSCRIBE

	// find existing subscribe
	auto req = self->req;
	Str* user;
	if (str_empty(&req->rel_user))
		user = &req->user->config->name;
	else
		user = &req->rel_user;
	auto sub = subscribe_mgr_find(&self->subscribe_mgr, user, &req->rel);
	if (sub)
		error("subscribe '%.*s': already exists", str_size(&req->rel),
		      str_of(&req->rel));

	// find relation
	auto rel = catalog_find(&share()->db->catalog, user, &req->rel, true);

	uint64_t lsn    = state_lsn();
	uint32_t lsn_op = 0;
	Uuid*    uuid;
	switch (rel->type) {
	case REL_SUBSCRIPTION:
	{
		auto sub = sub_of(rel);
		lsn    = sub->config->lsn;
		lsn_op = sub->config->op;
		uuid   = &sub->config->id;
		break;
	}
	case REL_TABLE:
	{
		uuid = &table_of(rel)->config->id;
		table_of(rel)->part_arg.cdc++;
		break;
	}
	case REL_TOPIC:
		uuid = &topic_of(rel)->config->id;
		break;
	default:
		error("subscribe '%.*s': unsupported relation type", str_size(&req->rel),
		      str_of(&req->rel));
		break;
	}

	// todo: check grants/ownership

	// create and register sub
	sub = subscribe_allocate();
	subscribe_set_user(sub, user);
	subscribe_set_name(sub, &req->rel);
	subscribe_mgr_add(&self->subscribe_mgr, sub);

	// open cursor
	cdc_slot_set(&sub->slot, lsn, lsn_op);
	cdc_cursor_open(&sub->cursor, share()->cdc, uuid, lsn, lsn_op);

	// reply
	Str column;
	str_set(&column, "subscribe", 9);

	// []
	uint8_t empty_array[8];
	auto pos = empty_array;
	json_write_array(&pos);
	json_write_array_end(&pos);
	output_write_json(&req->output, &column, empty_array, false);
}

hot static void
feed_execute(Feed* self, Str* content)
{
	auto req = self->req;

	// parse
	auto on_error = error_catch
	(
		// auth (take catalog lock)
		request_auth(req, &self->fe->auth);

		// parse
		request_rpc(req, content);
		if (req->type == REQUEST_SUBSCRIBE)
			feed_subscribe(self);
	);
	if (on_error)
	{
		output_write_error(&req->output, &am_self()->error);
		return;
	}

	// command handled by feed
	if (req->type == REQUEST_SUBSCRIBE)
		return;

	// execute
	self->fe->iface->session_execute(self->session, req);
}

hot static void
feed_collect(Feed* self)
{
	auto buf = self->req->output.buf;
	subscribe_mgr_collect(&self->subscribe_mgr, buf);
}

hot static inline bool
feed_wait(Feed* self)
{
	// if no subscriptions, go straight to io wait
	if (subscribe_mgr_empty(&self->subscribe_mgr))
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
	// get min lsn across all subscribers
	//
	uint64_t min = subscribe_mgr_min(&self->subscribe_mgr);
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
feed_recv(Feed* self, Str* content)
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
feed_send(Feed* self)
{
	auto buf = self->req->output.buf;
	if (buf_empty(buf))
		return;
	struct iovec iov;
	iov_set_buf(&iov, buf);
	websocket_send(&self->ws, WS_TEXT, &iov, 1, 0);
}

hot void
frontend_feed(Frontend* self, Client* client, Request* req, void* session)
{
	Feed feed;
	feed_init(&feed, self, client, req, session);
	defer(feed_free, &feed);
	feed_accept(&feed);

	for (;;)
	{
		// reset and unlock catalog
		feed_reset(&feed);

		// wait for client io or cdc event
		if (feed_wait(&feed))
		{
			// recv jsonrpc request
			Str content;
			if (! feed_recv(&feed, &content))
				break;
			// parse and execute
			feed_execute(&feed, &content);
		}

		// collect pending cdc events
		feed_collect(&feed);

		// batch send
		feed_send(&feed);
	}
}
