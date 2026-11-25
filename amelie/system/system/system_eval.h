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
	Request  req;
	Buf      req_buf;
	Session* session;
};

static inline SystemEval*
system_eval_allocate(void)
{
	auto self = (SystemEval*)am_malloc(sizeof(SystemEval));
	self->session = NULL;
	buf_init(&self->req_buf);
	request_init(&self->req);
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
	buf_free(&self->req_buf);
	request_free(&self->req);
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
	buf_reset(&self->req_buf);

	// todo: output to none
	auto req = &self->req;
	output_set_buf(&req->output, &self->req_buf);
	output_set(&req->output, &req->endpoint, &output_json, NULL);

	// update time and random seed
	request_prepare(req);

	// auth request
	request_bind_name(req, db);

	// set request
	str_set_str(&req->endpoint.content, command);
	req->replay = true;

	// execute
	session_execute(self->session, req);
	request_reset(req, true);
}
