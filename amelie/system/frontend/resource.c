
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
	self->args      = NULL;
	self->args_size = 0;
	self->portal    = portal;
	str_init(&self->rel_user);
	str_init(&self->rel);
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
	self->args      = NULL;
	self->args_size = 0;
	str_init(&self->rel_user);
	str_init(&self->rel);
	json_reset(&self->json);
}

static void
resource_parse_content(Resource* self, Str* content)
{
	auto json = &self->json;

	// todo: parse endpoint uri and find api

	// parse json body
	json_parse(json, content, NULL);

	self->args = json->buf->start;
	self->args_size = buf_size(json->buf);
}

bool
resource_parse(Resource* self, Str* content, Request* req)
{
	// parser jsonrpc request
	auto on_error = error_catch
	(
		resource_parse_content(self, content);
	);
	if (on_error)
	{
		output_error(&self->portal->output, &am_self()->error);
		return false;
	}

	req->type      = REQUEST_WRITE;
	req->rel_user  = self->rel_user;
	req->rel       = self->rel;
	req->args      = self->args;
	req->args_size = self->args_size;
	return true;
}
