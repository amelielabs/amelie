
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

hot void
frontend_feed(Frontend* self, Client* client, Request* req, void* session)
{
	Feed feed;
	feed_init(&feed, self, client, req, session);
	defer(feed_free, &feed);

	// websocket handshake
	websocket_accept(&feed.ws);

	auto buf = &client->request.content;
	for (;;)
	{
		request_reset(req, false);
		output_set(&req->output, &req->endpoint);

		// read jsonrpc request
		if (! websocket_recv(&feed.ws, NULL, 0))
			break;
		// todo: handle special types

		buf_reset(buf);
		websocket_recv_data(&feed.ws, buf);
		Str content;
		buf_str(buf, &content);

		// parse and execute
		feed_execute(&feed, &content);

		// reply
		struct iovec iov =
		{
			.iov_base = req->output.buf->start,
			.iov_len  = buf_size(req->output.buf)
		};
		websocket_send(&feed.ws, WS_TEXT, &iov, 1, 0);
	}
}
