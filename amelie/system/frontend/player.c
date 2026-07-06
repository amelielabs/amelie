
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
player_init(Player* self, PlayerSync* sync, Frontend* fe)
{
	self->sync = sync;
	self->fe   = fe;
	self->arg  = NULL;
	msg_init(&self->msg, MSG_PLAYER);
	msg_init(&self->msg_stop, MSG_PLAYER_STOP);
	request_init(&self->req);
	query_init(&self->query);
	mailbox_init(&self->queue);
	buf_init(&self->buf);
}

void
player_free(Player* self)
{
	request_free(&self->req);
	buf_free(&self->buf);
}

static inline void
player_record_primary(RecordMsg* record)
{
	// check replication state
	if (unlikely(! opt_int_of(&state()->repl)))
		error("player: replication is disabled");

	// ensure record comes from the current primary
	if (! uuid_is(&record->instance_id, opt_uuid_of(&state()->repl_primary)))
		error("player: primary id mismatch");
}

static void
player_record(Player* self, RecordMsg* record, void* session)
{
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
		// set user (takes shared catalog lock)
		request_auth_as(req, opt_string_of(&endpoint->user));

		// validate replicated record
		if (! uuid_is(&record->instance_id, opt_uuid_of(&config()->uuid)))
			player_record_primary(record);

		// execute
		self->fe->iface->session_execute(session, req, &self->query);
	);
	request_reset(req, true);
	if (on_error)
		rethrow();
}

void
player_main(Player* self)
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
			player_sync_add_shutdown(sync);
			break;
		}
		auto record = (RecordMsg*)msg;
		defer_buf(record->msg_buf);

		// process record
		auto on_error = error_catch
		(
			player_record(self, record, session);
		);

		if (on_error)
		{
			// todo: set player error
			player_sync_add_error(sync);
		}

		player_sync_add_complete(sync);
	}

	ctl->session_free(session);
}
