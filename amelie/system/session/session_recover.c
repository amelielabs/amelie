
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
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>

typedef struct RecoverSession RecoverSession;

struct RecoverSession
{
	Request  req;
	Query    query;
	Session* session;
	Buf      buf;
};

static void
recover_if_create(Recover* recover)
{
	auto self = (RecoverSession*)am_malloc(sizeof(RecoverSession));
	recover->state = self;

	// todo: recover session
	self->session = session_create();
	request_init(&self->req);
	query_init(&self->query);
	buf_init(&self->buf);
}

static void
recover_if_free(Recover* recover)
{
	if (! recover->state)
		return;
	auto self = (RecoverSession*)recover->state;
	session_free(self->session);
	request_free(&self->req);
	buf_free(&self->buf);
	am_free(self);
	recover->state = NULL;
}

static void
recover_if_execute(Recover* recover, Record* record)
{
	auto self     = (RecoverSession*)recover->state;
	auto req      = &self->req;
	auto endpoint = &req->endpoint;
	query_reset(&self->query);

	// restore query and request data
	query_read(&self->query, endpoint, record);

	// todo: output to none
	buf_reset(&self->buf);
	output_set_buf(&req->output, &self->buf);
	output_set(&req->output, endpoint, &output_json, NULL);

	// process request
	auto on_error = error_catch
	(	
		// set user
		request_auth_as(req, opt_string_of(&endpoint->user));

		// execute
		session_execute(self->session, req, &self->query);
	);
	request_reset(req, true);
	if (on_error)
		rethrow();
}

RecoverIf recover_if =
{
	.create  = recover_if_create,
	.free    = recover_if_free,
	.execute = recover_if_execute
};
