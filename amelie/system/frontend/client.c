
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
frontend_client_primary_on_write(Node* self, NodeMsg* msg, Buf* data)
{
	auto fe = (Frontend*)((void**)self->execute_arg)[0];
	fe->iface->session_execute_msg(((void**)self->execute_arg)[1], self, msg, data);
}

static void
frontend_client_primary(Frontend* self, Client* client, void* session)
{
	Recover recover;
	recover_init(&recover, share()->db, true);
	defer(recover_free, &recover);

	void* args[] = {self, session};
	Node node;
	node_init(&node, frontend_client_primary_on_write, args, &recover, client);
	node_main(&node);
}

hot static inline void
frontend_request(Frontend* self, Request* req, Client* client)
{
	unused(self);
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// /v1/sql (plain/text)
	if (opt_int_of(&endpoint->endpoint) == ENDPOINT_SQL)
	{
		if (! str_is(&endpoint->content_type.string, "plain/text", 10))
			error("unsupported operation content-type");
	}

	// set request content
	buf_str(&http->content, &req->content);
}

hot static inline bool
frontend_endpoint(Frontend* self, Request* req, Client* client)
{
	auto endpoint = &req->endpoint;
	auto http     = &client->request;

	// POST /v1/sql
	// POST /v1/backup
	// POST /v1/repl
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is(method, "POST", 4)))
		return false;

	// set content-type
	auto content_type = http_find(http, "Content-Type", 12);
	if (likely(content_type))
		endpoint->content_type.string = content_type->value;

	// set accept
	auto accept = http_find(http, "Accept", 6);
	if (likely(accept))
		endpoint->accept.string = accept->value;
	else
		str_set(&endpoint->accept.string, "application/json", 16);

	// set user
	auto user = http_find(http, "X-User-ID", 9);
	if (user)
		endpoint->user.string = user->value;
	else
		str_set(&endpoint->user.string, "main", 4);

	// set token
	auto auth = http_find(http, "Authorization", 13);
	if (auth)
		endpoint->token.string = auth->value;

	// if auth is required
	opt_int_set(&endpoint->auth, client->auth);

	// parse uri endpoint request
	return !error_catch
	(
		uri_parse_endpoint(endpoint, &http->options[HTTP_URL]);

		// configure output mime (using Accept)
		auto output = &req->output;
		output_reset(output);
		output_set(output, endpoint);
		output_set_buf(output, &client->reply.content);

		frontend_request(self, req, client);
	);
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
		request_reset(&req);

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
		if (! frontend_endpoint(self, &req, client))
		{
			// 400 Bad Request
			client_400(client, NULL);
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
			request_reset(&req);
			return backup(share()->db, client);
		}
		case ENDPOINT_REPL:
		{
			// primary connection
			request_reset(&req);
			return frontend_client_primary(self, client, session);
		}
		default:
			abort();
			break;
		}
	}
}
