
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

	// POST /sql (text/plain)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "text/plain", 10) &&
	    !str_is(content_type, "application/x-www-form-urlencoded", 33))
		error("unsupported operation content-type");

	// accept
	OutputIf* output_if;
	auto accept = &endpoint->accept.string;
	if (str_empty(accept) ||
	    str_is(accept, "*/*", 3) ||
	    str_is(accept, "text/plain", 10))
	{
		str_set(accept, "text/plain", 10);
		output_if = &output_text;
	} else
	if (str_is(accept, "application/json", 16)) {
		output_if = &output_json;
	} else {
		error("unsupported operation accept type");
	}

	// set output type
	output_set(&req->output, endpoint, output_if, NULL);
}

hot static inline void
frontend_endpoint_api(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /api (application/json)
	// GET  /api (application/json) (websocket)

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "application/json", 16))
		error("unsupported operation content-type");

	// accept (jsonrpc)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "application/json", 16) &&
	    !str_is(accept, "*/*", 3))
		error("unsupported operation accept");

	str_set(accept, "application/json", 16);

	// set output type
	output_set(&req->output, endpoint, &output_jsonrpc, NULL);

	// check method
	auto method = &http->options[HTTP_METHOD];
	if (str_is(method, "POST", 4))
		return;
	if (! str_is(method, "GET", 3))
		error("unsupported operation method");
}

hot static inline void
frontend_endpoint_mcp(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /mcp (application/json)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "application/json", 16))
		error("unsupported operation content-type");

	// accept (jsonrpc)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "application/json", 16) &&
	    !str_is(accept, "*/*", 3))
		error("unsupported operation accept");

	str_set(accept, "application/json", 16);

	// set output type
	output_set(&req->output, endpoint, &output_jsonrpc, NULL);
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

	// accept
	auto accept = &endpoint->accept.string;
	str_set(accept, "application/json", 16);

	// set output type
	output_set(&req->output, endpoint, &output_json, NULL);
}

hot static inline bool
frontend_endpoint(Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /sql
	// POST /api
	// GET  /api (websocket)
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
		str_set(&endpoint->user.string, "amelie", 6);

	// token
	auto auth = http_find(http, "Authorization", 13);
	if (auth)
		endpoint->token.string = auth->value;

	// update time and random seed
	request_prepare(req);

	// if auth is required
	opt_int_set(&endpoint->trusted, client->trusted);

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
		if (endpoint_type == ENDPOINT_API)
			frontend_endpoint_api(req, client);
		else
		if (endpoint_type == ENDPOINT_MCP)
			frontend_endpoint_mcp(req, client);
		else
			frontend_endpoint_service(req, client);
	);
	if (on_error)
	{
		if (output->iface)
			output_error(output, &am_self()->error);
	}

	return !on_error;
}

hot static inline bool
frontend_auth(Frontend* self, Request* req)
{
	// take catalog lock and authenticate user
	return !error_catch (
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

	Query query;
	query_init(&query);

	Api api;
	api_init(&api, &req);
	defer(api_free, &api);

	Mcp mcp;
	mcp_init(&mcp, &req);
	defer(mcp_free, &mcp);

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
		Str content;
		buf_str(&http->content, &content);

		// parse endpoint request
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
		auto endpoint = opt_int_of(&req.endpoint.endpoint);
		switch (endpoint) {
		case ENDPOINT_API:
		{
			// switch to the websocket session
			if (str_is(&http->options[HTTP_METHOD], "GET", 3))
			{
				request_reset(&req, false);
				return frontend_follower(self, client, &req, &api, session);
			}

			// parse api request
			query_reset(&query);
			api_reset(&api);
			if (! api_parse(&api, &content, &query, false))
			{
				// 400 Bad Request
				client_400(client, req.output.buf);
				break;
			}

			// execute request
			if (query.type != QUERY_UNDEF)
				ctl->session_execute(session, &req, &query);

			// 200 OK (includes errors)
			if (buf_empty(req.output.buf))
				output_none(&req.output);
			client_200(client, req.output.buf);
			break;
		}
		case ENDPOINT_MCP:
		{
			// parse mcp request
			query_reset(&query);
			mcp_reset(&mcp);
			if (! mcp_parse(&mcp, &content, &query))
			{
				// 400 Bad Request
				client_400(client, req.output.buf);
				break;
			}

			// execute request
			if (query.type != QUERY_UNDEF)
				ctl->session_execute(session, &req, &query);

			// 200 OK (includes errors)
			if (buf_empty(req.output.buf))
				output_none(&req.output);
			client_200(client, req.output.buf);
			break;
		}
		case ENDPOINT_SQL:
		{
			// execute query
			query.type = QUERY_SQL;
			query.text = content;
			if (ctl->session_execute(session, &req, &query))
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
			// ensure server is replica
			if (state_is_primary())
			{
				// todo: change code
				client_400(client, NULL);
				error("server is not a replica");
				return;
			}

			// unlock
			request_reset(&req, true);

			// process by receiver (wait for completion)
			client_detach(client);
			receiver_send(&share()->repl->receiver, client);
			return;
		}
		default:
			abort();
			break;
		}
	}
}
