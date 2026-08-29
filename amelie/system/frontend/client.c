
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

hot static inline void
frontend_endpoint_sql(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
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
	output_set(&portal->output, endpoint, output_if, NULL);
}

hot static inline void
frontend_endpoint_import(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
	auto http     = &client->request;

	// POST /import (text/plain)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "text/plain", 10) &&
	    !str_is(content_type, "text/csv", 8) &&
	    !str_is(content_type, "application/x-www-form-urlencoded", 33))
		error("unsupported operation content-type");

	// accept
	OutputIf* output_if;
	auto accept = &endpoint->accept.string;
	if (str_empty(accept) ||
	    str_is(accept, "*/*", 3)         ||
	    str_is(accept, "text/plain", 10) ||
	    str_is(accept, "text/csv", 8))
	{
		str_set(accept, "text/plain", 10);
		output_if = &output_text;
	} else
	if (str_is(accept, "application/json", 16)) {
		output_if = &output_json;
	} else {
		error("unsupported operation accept type");
	}

	// target
	auto target = opt_string_of(&endpoint->target);
	if (str_empty(target))
		error("target argument is missing");

	// set output type
	output_set(&portal->output, endpoint, output_if, NULL);
}

hot static inline void
frontend_endpoint_api(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
	auto http     = &client->request;

	// POST /api (application/json)

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
	output_set(&portal->output, endpoint, &output_jsonrpc, NULL);

	// check method
	auto method = &http->options[HTTP_METHOD];
	if (str_is(method, "POST", 4))
		return;
	if (! str_is(method, "GET", 3))
		error("unsupported operation method");
}

hot static inline void
frontend_endpoint_stream(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
	auto http     = &client->request;

	// GET /stream (text/event-stream) SSE
	auto content_type = &endpoint->content_type.string;
	str_set(content_type, "text/event-stream", 17);

	// accept (text/event-stream)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "text/event-stream", 17) &&
	    !str_is(accept, "*/*", 3))
		error("unsupported operation accept");

	str_set(accept, "application/json", 16);

	// set output type (only for errors)
	output_set(&portal->output, endpoint, &output_json, NULL);

	// check method
	auto method = &http->options[HTTP_METHOD];
	if (! str_is(method, "GET", 3))
		error("unsupported operation method");
}

hot static inline void
frontend_endpoint_mcp(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
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
	output_set(&portal->output, endpoint, &output_jsonrpc, NULL);
}

hot static inline void
frontend_endpoint_service(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
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
	output_set(&portal->output, endpoint, &output_json, NULL);
}

hot static inline bool
frontend_endpoint(Portal* portal, Client* client)
{
	auto endpoint = &portal->endpoint;
	auto http     = &client->request;

	// POST /sql
	// POST /api
	// GET  /stream
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
	portal_prepare(portal);

	// if auth is required
	opt_int_set(&endpoint->trusted, client->trusted);

	// parse uri endpoint request
	auto output = &portal->output;
	output_reset(output);
	output_set_buf(output, &client->reply.content);

	// /<endpoint>
	auto on_error = error_catch
	(
		uri_parse_endpoint(endpoint, &http->options[HTTP_URL]);

		auto endpoint_type = opt_int_of(&endpoint->endpoint);
		if (endpoint_type == ENDPOINT_SQL)
			frontend_endpoint_sql(portal, client);
		else
		if (endpoint_type == ENDPOINT_IMPORT)
			frontend_endpoint_import(portal, client);
		else
		if (endpoint_type == ENDPOINT_STREAM)
			frontend_endpoint_stream(portal, client);
		else
		if (endpoint_type == ENDPOINT_API)
			frontend_endpoint_api(portal, client);
		else
		if (endpoint_type == ENDPOINT_MCP)
			frontend_endpoint_mcp(portal, client);
		else
			frontend_endpoint_service(portal, client);
	);
	if (on_error)
	{
		if (output->iface)
			output_error(output, &am_self()->error);
	}

	return !on_error;
}

hot static inline bool
frontend_auth(Frontend* self, Portal* portal)
{
	// take catalog lock and authenticate user
	return !error_catch (
		portal_auth(portal, &self->auth);
	);
}

void
frontend_client(Frontend* self, Client* client)
{
	auto readahead = &client->readahead;
	auto http      = &client->request;

	// prepare portal and request
	Portal portal;
	portal_init(&portal);
	defer(portal_free, &portal);
	client_set_endpoint(client, &portal.endpoint);

	Request req;
	request_init(&req);

	Api api;
	api_init(&api, &portal);
	defer(api_free, &api);

	Mcp mcp;
	mcp_init(&mcp, &portal);
	defer(mcp_free, &mcp);

	// create sesssion
	auto ctl = self->iface;
	auto session = ctl->session_create(self, self->iface_arg);
	defer(ctl->session_free, session);

	for (;;)
	{
		portal_reset(&portal, true);

		// read header
		http_reset(http);
		auto eof = http_read(http, readahead, true);
		if (unlikely(eof))
			break;

		// read content
		auto limit = opt_int_of(&config()->recv);
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
		if (! frontend_endpoint(&portal, client))
		{
			// 400 Bad Source
			client_400(client, portal.output.buf);
			continue;
		}

		// authenticate user
		if (! frontend_auth(self, &portal))
		{
			// 403 Forbidden
			client_403(client);
			continue;
		}

		// execute
		auto endpoint = opt_int_of(&portal.endpoint.endpoint);
		switch (endpoint) {
		case ENDPOINT_API:
		{
			// parse api request
			request_reset(&req);
			api_reset(&api);
			if (! api_parse(&api, &content, &req))
			{
				// 400 Bad Source
				client_400(client, portal.output.buf);
				break;
			}

			// execute request
			if (req.type != REQUEST_UNDEF)
				ctl->session_execute(session, &portal, &req);

			// 200 OK (includes errors)
			if (buf_empty(portal.output.buf))
				output_none(&portal.output);
			client_200(client, portal.output.buf);
			break;
		}
		case ENDPOINT_MCP:
		{
			// parse mcp request
			request_reset(&req);
			mcp_reset(&mcp);
			if (! mcp_parse(&mcp, &content, &req))
			{
				// 400 Bad Source
				client_400(client, portal.output.buf);
				break;
			}

			// execute request
			if (req.type != REQUEST_UNDEF)
				ctl->session_execute(session, &portal, &req);

			// 200 OK (includes errors)
			if (buf_empty(portal.output.buf))
				output_none(&portal.output);
			client_200(client, portal.output.buf);
			break;
		}
		case ENDPOINT_SQL:
		{
			// execute query
			req.type = REQUEST_SQL;
			req.text = content;
			if (ctl->session_execute(session, &portal, &req))
			{
				// 204 No Content
				// 200 OK
				if (buf_empty(portal.output.buf))
					client_204(client);
				else
					client_200(client, portal.output.buf);
				break;
			}

			// 400 Bad Source
			client_400(client, portal.output.buf);
			break;
		}
		case ENDPOINT_IMPORT:
		{
			// import content
			req.type = REQUEST_IMPORT;
			req.args = str_u8(&content);
			req.args_size = str_size(&content);
			str_init(&req.rel_user);
			str_init(&req.rel);

			// set target
			auto target = opt_string_of(&portal.endpoint.target);
			auto pos = target->pos;
			auto end = target->end;
			if (! portal_target(&pos, end, &req.rel_user, &req.rel))
				error("failed to read target");

			if (ctl->session_execute(session, &portal, &req))
			{
				// 204 No Content
				// 200 OK
				if (buf_empty(portal.output.buf))
					client_204(client);
				else
					client_200(client, portal.output.buf);
				break;
			}

			// 400 Bad Source
			client_400(client, portal.output.buf);
			break;
		}
		case ENDPOINT_STREAM:
		{
			// /stream
			//
			// pass to the SSE processing (portal keeps lock)
			//
			frontend_stream(self, client, &portal);
			return;
		}
		case ENDPOINT_BACKUP:
		{
			// restore connection (remote backup)
			portal_reset(&portal, true);
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
			portal_reset(&portal, true);

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
