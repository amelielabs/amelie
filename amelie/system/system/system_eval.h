#pragma once

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

typedef struct SystemEval SystemEval;

struct SystemEval
{
	Portal   portal;
	Buf      portal_buf;
	Request  request; 
	Session* session;
};

static inline SystemEval*
system_eval_allocate(void)
{
	auto self = (SystemEval*)am_malloc(sizeof(SystemEval));
	self->session = NULL;
	buf_init(&self->portal_buf);
	portal_init(&self->portal);
	request_init(&self->request);
	return self;
}

static inline void
system_eval_free(SystemEval* self)
{
	if (self->session)
	{
		session_free(self->session);
		self->session = NULL;
	}
	buf_free(&self->portal_buf);
	portal_free(&self->portal);
	am_free(self);
}

static inline void
system_eval_create(SystemEval* self)
{
	// create session
	self->session = session_create();
}

static inline void
system_eval(SystemEval* self, Str* command)
{
	buf_reset(&self->portal_buf);

	// todo: output to none
	auto portal = &self->portal;
	output_set_buf(&portal->output, &self->portal_buf);
	output_set(&portal->output, &portal->endpoint, &output_json, NULL);

	// auth portal
	Str user;
	str_set(&user, "amelie", 6);
	portal_auth_as(portal, &user, false);

	// set request
	auto request = &self->request;	
	request->type = REQUEST_SQL;
	str_set_str(&request->text, command);

	// execute
	session_execute(self->session, portal, request);
	portal_reset(portal, true);
}
