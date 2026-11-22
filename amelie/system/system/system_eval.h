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
	Session* session;
};

static inline SystemEval*
system_eval_allocate(void)
{
	auto self = (SystemEval*)am_malloc(sizeof(SystemEval));
	self->session = NULL;
	buf_init(&self->portal_buf);
	portal_init(&self->portal);
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
system_eval(SystemEval* self, Str* db, Str* command)
{
	buf_reset(&self->portal_buf);

	// todo: output to none
	auto portal = &self->portal;
	output_set_buf(&portal->output, &self->portal_buf);
	output_set(&portal->output, &portal->endpoint, &output_json, NULL);

	// update time and random seed
	portal_prepare(portal);

	// auth portal
	portal_bind_name(portal, db);

	// set request
	auto req = &portal->req;
	request_set_replay(req);
	str_set_str(&req->content, command);

	// execute
	session_execute(self->session, portal);
	portal_reset(portal, true);
}
