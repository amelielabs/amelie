
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

void
session_execute_replay(Session* self, Primary* primary, Buf* data)
{
	auto lock = lock_system(LOCK_CATALOG, LOCK_SHARED);
	defer(unlock, lock);

	// validate request fields and check current replication state

	// first join request has no data
	if (! primary_next(primary))
		return;

	// switch distributed transaction to replication state to write wal
	// while in read-only mode
	auto dtr = &self->dtr;
	auto program = self->program;
	program->repl = true;

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

		switch (record_cmd(record)->cmd) {
		case CMD_REPLACE:
		case CMD_DELETE:
		{
			// execute DML
			dtr_reset(dtr);
			dtr_create(dtr, program);
			replay(dtr, record);
			break;
		}
		case CMD_DDL:
		{
			// upgrade to the exclusive lock
			unlock(lock);

			// (unlocked by recover commit)
			lock_system(LOCK_CATALOG, LOCK_EXCLUSIVE);

			// execute DDL
			recover_next(primary->recover, record);
			break;
		}
		case CMD_DDL_CREATE_INDEX:
		{
			unlock(lock);

			// execute CREATE INDEX
			recover_next(primary->recover, record);
			break;
		}
		}
		pos += record->size;
	}
}
