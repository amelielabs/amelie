
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
resource_init(Resource* self, Portal* portal)
{
	self->portal = portal;
	json_init(&self->json);
}

void
resource_free(Resource* self)
{
	json_free(&self->json);
}

void
resource_reset(Resource* self)
{
	json_reset(&self->json);
}

static void
resource_parse_content(Resource* self, Str* content, Request* req)
{
	auto json = &self->json;
	auto portal = self->portal;

	// todo: parse endpoint uri and find api
	auto uri = opt_string_of(&portal->endpoint.endpoint);
	auto api = apis_find(&portal->user->config->apis, uri);
	if (! api)
		error("user {str}: api '{str}' not found",
		      &portal->user->config->name, uri);

	// parse json body
	json_parse(json, content, NULL);

	// set request
	req->type      = REQUEST_WRITE;
	req->rel_user  = api->rel_user;
	req->rel       = api->rel;
	req->args      = json->buf->start;
	req->args_size = buf_size(json->buf);
}

bool
resource_parse(Resource* self, Str* content, Request* req)
{
	// parser jsonrpc request
	auto on_error = error_catch
	(
		resource_parse_content(self, content, req);
	);
	if (on_error)
	{
		output_error(&self->portal->output, &am_self()->error);
		return false;
	}
	return true;
}
