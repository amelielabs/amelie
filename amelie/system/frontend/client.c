
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

hot static inline void
frontend_endpoint_sql(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /sql (plain/text)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) && !str_is(content_type, "plain/text", 10))
		error("unsupported operation content-type");

	// accept (set default)
	auto accept = &endpoint->accept.string;
	if (str_empty(accept))
		str_set(accept, "application/json", 16);

	// set output
	output_set(&req->output, endpoint);

	Str content;
	buf_str(&http->content, &content);

	// sql command
	req->type = REQUEST_SQL;
	req->text = content;
}

hot static inline void
frontend_endpoint_rpc(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /rpc (application/json)
	// POST /rpc (application/json-rpc)
	// GET  /rpc (application/json)      (websocket)
	// GET  /rpc (application/json-rpc)  (websocket)

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "application/json", 16) &&
	    !str_is(content_type, "application/json-rpc", 20))
		error("unsupported operation content-type");

	// accept (jsonrpc)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "application/json", 16) &&
	    !str_is(accept, "application/json-rpc", 20))
		error("unsupported operation accept");
	str_set(accept, "application/json-rpc", 20);

	// set output
	output_set(&req->output, endpoint);

	// parse jsonrpc request
	auto method = &http->options[HTTP_METHOD];
	if (str_is(method, "POST", 4))
	{
		Str content;
		buf_str(&http->content, &content);
		request_rpc(req, &content);
	} else
	if (! str_is(method, "GET", 3)) {
		error("unsupported operation method");
	}
}

hot static inline void
frontend_endpoint_service(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// GET /backup
	// GET /repl
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "GET", 3)))
		error("unsupported operation method");

	// ignoring content-type

	// accept (jsonrpc)
	auto accept = &endpoint->accept.string;
	str_set(accept, "application/json", 16);

	// set output
	output_set(&req->output, endpoint);
}

hot static inline bool
frontend_endpoint(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /sql
	// POST /rpc
	// GET  /rpc (websocket)
	// GET  /backup
	// GET  /repl

	// content type
	auto content_type = http_find(http, "Content-Type", 12);
	if (likely(content_type))
		endpoint->content_type.string = content_type->value;

	// accept
	auto accept = http_find(http, "Accept", 6);
	if (likely(accept))
		endpoint->accept.string = accept->value;

	// user
	auto user = http_find(http, "X-User-ID", 9);
	if (user)
		endpoint->user.string = user->value;
	else
		str_set(&endpoint->user.string, "main", 4);

	// token
	auto auth = http_find(http, "Authorization", 13);
	if (auth)
		endpoint->token.string = auth->value;

	// if auth is required
	opt_int_set(&endpoint->auth, client->auth);

	// parse uri endpoint request
	auto output = &req->output;
	output_reset(output);
	output_set_buf(output, &client->reply.content);

	// /<endpoint>
	auto on_error = error_catch
	(
		uri_parse_endpoint(endpoint, &http->options[HTTP_URL]);

		auto endpoint_type = opt_int_of(&endpoint->endpoint);
		if (endpoint_type == ENDPOINT_SQL)
			frontend_endpoint_sql(req, client);
		else
		if (endpoint_type == ENDPOINT_RPC)
			frontend_endpoint_rpc(req, client);
		else
			frontend_endpoint_service(req, client);
	);

	if (on_error)
	{
		if (output->iface)
			output_write_error(output, &am_self()->error);
	}

	return !on_error;
}

hot static inline bool
frontend_auth(Frontend* self, Request* req)
{
	// take catalog lock and authenticate user
	return !error_catch
	(
		request_auth(req, &self->auth);
	);
}

void
frontend_client(Frontend* self, Client* client)
{
	auto readahead = &client->readahead;
	auto http      = &client->request;

	// prepare request
	Request req;
	request_init(&req);
	defer(request_free, &req);
	client_set_endpoint(client, &req.endpoint);

	// create sesssion
	auto ctl = self->iface;
	auto session = ctl->session_create(self, self->iface_arg);
	defer(ctl->session_free, session);
	for (;;)
	{
		request_reset(&req, true);

		// read header
		http_reset(http);
		auto eof = http_read(http, readahead, true);
		if (unlikely(eof))
			break;

		// read content
		auto limit = opt_int_of(&config()->limit_recv);
		auto limit_reached =
			http_read_content_limit(http, readahead, &http->content, limit);
		if (unlikely(limit_reached))
		{
			// 413 Payload Too Large
			client_413(client);
			break;
		}

		// parse endpoint and configure request
		if (! frontend_endpoint(&req, client))
		{
			// 400 Bad Request
			client_400(client, req.output.buf);
			continue;
		}

		// authenticate user
		if (! frontend_auth(self, &req))
		{
			// 403 Forbidden
			client_403(client);
			continue;
		}

		// execute
		switch (opt_int_of(&req.endpoint.endpoint)) {
		case ENDPOINT_RPC:
		{
			// websocket session
			if (str_is(&http->options[HTTP_METHOD], "GET", 3))
			{
				request_reset(&req, false);
				return frontend_client_ws(self, client, &req, session);
			}

			// fallthrough
		}
		case ENDPOINT_SQL:
		{
			if (ctl->session_execute(session, &req))
			{
				// 204 No Content
				// 200 OK
				if (buf_empty(req.output.buf))
					client_204(client);
				else
					client_200(client, req.output.buf);
				break;
			}

			// 400 Bad Request
			client_400(client, req.output.buf);
			break;
		}
		case ENDPOINT_BACKUP:
		{
			// restore connection (remote backup)
			request_reset(&req, true);
			return backup(share()->db, client);
		}
		case ENDPOINT_REPL:
		{
			// primary connection
			request_reset(&req, true);
			return frontend_client_primary(self, client, session);
		}
		default:
			abort();
			break;
		}
	}
}
