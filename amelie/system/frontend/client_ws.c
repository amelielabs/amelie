
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
		buf_reset(buf);

		// read jsonrpc request
		if (! websocket_recv(&ws, NULL, 0))
			break;
		readahead_recv_buf(&client->readahead, buf, ws.frame.size);

		// auth (take catalog lock)
		request_auth(req, &self->auth);

		// parse
		Str content;
		buf_str(buf, &content);
		request_rpc(req, &content);

		// execute
		ctl->session_execute(session, req);

		// reply
		struct iovec iov =
		{
			.iov_base = req->output.buf->start,
			.iov_len  = buf_size(req->output.buf)
		};
		websocket_send(&ws, WS_TEXT, &iov, 1, 0);
	}
}
