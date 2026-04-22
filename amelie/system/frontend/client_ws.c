
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

hot static void
frontend_subscribe(Frontend* self, Request* req)
{
	(void)self;
	(void)req;
}

hot static void
frontend_client_execute(Frontend* self, Request* req, Str* content, void* session)
{
	// parse
	auto on_error = error_catch
	(
		// auth (take catalog lock)
		request_auth(req, &self->auth);

		// parse
		request_rpc(req, content);
		if (req->type == REQUEST_SUBSCRIBE)
			frontend_subscribe(self, req);
	);
	if (on_error)
	{
		output_write_error(&req->output, &am_self()->error);
		return;
	}

	if (req->type != REQUEST_SUBSCRIBE)
		return;

	// execute
	self->iface->session_execute(session, req);
}

hot void
frontend_client_ws(Frontend* self, Client* client, Request* req, void* session)
{
	// switch to websocket
	Websocket ws;
	websocket_init(&ws);
	defer(websocket_free, &ws);
	Str protocol;
	str_set(&protocol, "amelie-v1", 9);
	websocket_set(&ws, &protocol, client, false);
	websocket_accept(&ws);

	auto buf = &client->request.content;
	for (;;)
	{
		request_reset(req, false);
		output_set(&req->output, &req->endpoint);

		// read jsonrpc request
		if (! websocket_recv(&ws, NULL, 0))
			break;
		// todo: handle special types

		buf_reset(buf);
		websocket_recv_data(&ws, buf);
		Str content;
		buf_str(buf, &content);

		// parse and execute
		frontend_client_execute(self, req, &content, session);

		// reply
		struct iovec iov =
		{
			.iov_base = req->output.buf->start,
			.iov_len  = buf_size(req->output.buf)
		};
		websocket_send(&ws, WS_TEXT, &iov, 1, 0);
	}
}
