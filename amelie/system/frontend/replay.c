
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

void
replay_init(Replay* self, ReplaySync* sync, Frontend* fe)
{
	self->sync = sync;
	self->fe   = fe;
	self->arg  = NULL;
	msg_init(&self->msg, MSG_REPLAY);
	msg_init(&self->msg_stop, MSG_REPLAY_STOP);
	request_init(&self->req);
	query_init(&self->query);
	mailbox_init(&self->queue);
	buf_init(&self->buf);
}

void
replay_free(Replay* self)
{
	request_free(&self->req);
	buf_free(&self->buf);
}

static void
replay_record(Replay* self, RecordMsg* record, void* session)
{
	auto req      = &self->req;
	auto endpoint = &req->endpoint;
	query_reset(&self->query);

	// restore query and request data
	query_read(&self->query, endpoint, record->record);

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
		self->fe->iface->session_execute(session, req, &self->query);
	);
	request_reset(req, true);
	if (on_error)
		rethrow();
}

void
replay_main(Replay* self)
{
	auto fe      = self->fe;
	auto sync    = self->sync;
	auto ctl     = fe->iface;
	auto session = ctl->session_create(fe, fe->iface_arg);

	for (;;)
	{
		auto msg = mailbox_pop(&self->queue, am_self());
		if (msg == &self->msg_stop)
		{
			replay_sync_add_shutdown(sync);
			break;
		}
		auto record = (RecordMsg*)msg;
		defer_buf(record->msg_buf);

		// process record
		auto on_error = error_catch
		(
			replay_record(self, record, session);
		);

		if (on_error)
		{
			// todo: set replay error
			replay_sync_add_error(sync);
		}

		replay_sync_add_complete(sync);
	}

	ctl->session_free(session);
}
