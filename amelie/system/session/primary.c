
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>

hot static void
on_write(Primary* self, Buf* data)
{
	Session* session = self->replay_arg;

	// take shared lock
	session_lock(session, LOCK);
	defer(session_unlock, session);

	// validate request fields and check current replication state

	// first join request has no data
	if (! primary_next(self))
		return;

	// switch distributed transaction to replication state to write wal
	// while in read-only mode
	auto dtr = &session->dtr;
	auto program = session->compiler.program;
	program->sends = 1;
	program->repl  = true;

	// replay writes
	auto pos = data->start;
	auto end = data->position;
	while (pos < end)
	{
		auto record = (Record*)pos;
		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate(record)))
				error("repl: record crc mismatch, system LSN is: %" PRIu64,
				      state_lsn());

		if (likely(record_cmd_is_dml(record_cmd(record))))
		{
			// execute DML
			dtr_reset(dtr);
			dtr_create(dtr, program);
			replay(session->share, dtr, record);
		} else
		{
			// upgrade to exclusive lock
			session_lock(session, LOCK_EXCLUSIVE);

			// execute DDL
			recover_next(self->recover, record);
		}
		pos += record->size;
	}
}

hot void
session_primary(Session* self)
{
	auto share = self->share;

	Recover recover;
	recover_init(&recover, share->db, true, &build_if, share->backend_mgr);
	defer(recover_free, &recover);

	Primary primary;
	primary_init(&primary, &recover, self->client, on_write, self);
	primary_main(&primary);
}
