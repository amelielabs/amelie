
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

hot static void
feed_subscribe(Feed* self)
{
	(void)self;
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

	if (req->type != REQUEST_SUBSCRIBE)
		return;

	// execute
	self->fe->iface->session_execute(self->session, req);
}

static inline void
feed_accept(Feed* self)
{
	// websocket handshake
	websocket_accept(&self->ws);
}

static inline bool
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
	struct iovec iov;
	iov_set_buf(&iov, self->req->output.buf);
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

		// recv jsonrpc request
		Str content;
		if (! feed_recv(&feed, &content))
			break;

		// parse and execute
		feed_execute(&feed, &content);

		// batch send
		feed_send(&feed);
	}
}
