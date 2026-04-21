
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

	auto ctl = self->iface;
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

		// auth (take catalog lock)
		request_auth(req, &self->auth);

		// parse
		auto on_error = error_catch
		(
			Str content;
			buf_str(buf, &content);
			request_rpc(req, &content);
		);
		if (on_error)
		{
			output_write_error(&req->output, &am_self()->error);
		} else
		{
			// execute
			ctl->session_execute(session, req);
		}

		// reply
		struct iovec iov =
		{
			.iov_base = req->output.buf->start,
			.iov_len  = buf_size(req->output.buf)
		};
		websocket_send(&ws, WS_TEXT, &iov, 1, 0);
	}
}
