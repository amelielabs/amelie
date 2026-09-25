
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

void
link_init(Link* self, Frontend* fe, Client* client)
{
	self->client = client;
	self->api    = NULL;
	self->fe     = fe;

	portal_init(&self->portal);
	request_init(&self->req);
	mcp_init(&self->mcp, &self->portal);
	json_init(&self->json);
}

void
link_free(Link* self)
{
	mcp_free(&self->mcp);
	json_free(&self->json);
	portal_free(&self->portal);
}

hot static inline bool
link_auth(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// content type
	auto content_type = http_find(http, "Content-Type", 12);
	if (likely(content_type))
		endpoint->content_type.string = content_type->value;

	// accept
	auto accept = http_find(http, "Accept", 6);
	if (likely(accept))
		endpoint->accept.string = accept->value;

	// token
	auto auth = http_find(http, "Authorization", 13);
	if (auth)
		endpoint->token.string = auth->value;

	// update time and random seed
	portal_prepare(portal);

	// if auth is required
	opt_int_set(&endpoint->trusted, self->client->trusted);

	// authenticate user (and take catalog lock)
	return !error_catch (
		portal_auth(portal, &self->fe->auth);
	);
}

hot void
link_main(Link* self)
{
	auto client    = self->client;
	auto readahead = &client->readahead;
	auto http      = &client->request;
	auto portal    = &self->portal;
	client_set_endpoint(client, &portal->endpoint);

	// create sesssion
	auto ctl = self->fe->iface;
	auto session = ctl->session_create(self->fe, self->fe->iface_arg);
	defer(ctl->session_free, session);
	for (;;)
	{
		self->api = NULL;
		portal_reset(portal, true);

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
			// 413 Payload Too Large (disconnect)
			client_413(client);
			break;
		}

		// authenticate
		if (! link_auth(self))
		{
			// 401 Unauthorized (disconnect)
			client_401(client);
			break;
		}

		// read request
		switch (link_request(self)) {
		case LINK_ERROR:
		{
			// 400 Bad Request
			client_400(client, portal->output.buf);
			break;
		}
		case LINK_EXECUTE:
		{
			auto done = self->req.type == REQUEST_UNDEF;
			if (! done)
				done = ctl->session_execute(session, portal, &self->req);
			if (done)
			{
				// 204 No Content
				// 200 OK
				if (buf_empty(portal->output.buf))
					client_204(client);
				else
					client_200(client, portal->output.buf);
				break;
			}

			// 400 Bad Request
			client_400(client, portal->output.buf);
			break;
		}
		case LINK_FEED:
		{
			// SSE
			link_feed(self);
			return;
		}
		}
	}
}

void
frontend_link(Frontend* self, Client* client)
{
	Link link;
	link_init(&link, self, client);
	defer(link_free, &link);
	link_main(&link);
}
