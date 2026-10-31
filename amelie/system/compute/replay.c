
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
#include <amelie_compute.h>

void
replay_init(Replay* self, Compute* compute)
{
	self->session = NULL;
	self->compute = compute;
	portal_init(&self->portal);
	request_init(&self->req);
	buf_init(&self->buf);
}

void
replay_free(Replay* self)
{
	if (self->session)
	{
		self->compute->iface->session_free(self->session);
		self->session = NULL;
	}
	portal_free(&self->portal);
	buf_free(&self->buf);
}

#if 0
static inline void
replay_record_primary(RecordMsg* record)
{
	// ensure record comes from the current primary
	if (! uuid_is(&record->instance_id, opt_uuid_of(&state()->primary)))
		error("replay: primary id mismatch");
}
#endif

void
replay_run(Replay* self, RecordMsg* record)
{
	auto compute = self->compute;
	auto iface   = self->compute->iface;
	if (! self->session)
		self->session = iface->session_create(compute, compute->iface_arg);

	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	request_reset(&self->req);

	// todo: output to none
	buf_reset(&self->buf);
	output_set_buf(&portal->output, &self->buf);
	output_set(&portal->output, endpoint, &output_json, NULL);

	// process request
	auto on_error = error_catch
	(	
		// restore request
		request_read(&self->req, endpoint, record);

		// set user (takes shared catalog lock)

		// ignore user limits during recovery
		portal_set(portal, opt_string_of(&endpoint->user), false);

#if 0
		// validate replicated record
		if (! uuid_is(&record->instance_id, opt_uuid_of(&config()->uuid)))
			replay_record_primary(record);
#endif

		// execute
		compute->iface->session_execute(self->session, portal, &self->req);
	);

	portal_reset(portal, true);

	// sync
	RecoverSync* sync = record->arg;
	if (on_error)
		recover_sync_add_error(sync);
	recover_sync_add_complete(sync);
}
