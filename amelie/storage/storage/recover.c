
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
recover_init(Recover* self, Storage* storage, bool write_wal)
{
	self->ops       = 0;
	self->size      = 0;
	self->write_wal = write_wal;
	self->storage   = storage;
	tr_init(&self->tr);
	write_init(&self->write);
	write_list_init(&self->write_list);
}

void
recover_free(Recover* self)
{
	tr_free(&self->tr);
	write_free(&self->write);
}

static inline void
recover_reset_stats(Recover* self)
{
	self->ops  = 0;
	self->size = 0;
}

hot static void
recover_cmd(Recover* self, RecordCmd* cmd, uint8_t** pos)
{
	auto storage = self->storage;
	auto tr = &self->tr;
	switch (cmd->cmd) {
	case CMD_REPLACE:
	{
		// find partition by id
		auto part = part_mgr_find(&storage->part_mgr, cmd->partition);
		if (! part)
			error("recover: failed to find partition %" PRIu64, cmd->partition);
		auto end = *pos + cmd->size;
		while (*pos < end)
		{
			auto row = row_copy(&part->heap, (Row*)*pos);
			part_insert(part, tr, true, row);
			*pos += row_size(row);
		}
		break;
	}
	case CMD_DELETE:
	{
		// find partition by id
		auto part = part_mgr_find(&storage->part_mgr, cmd->partition);
		if (! part)
			error("recover: failed to find partition %" PRIu64, cmd->partition);
		auto end = *pos + cmd->size;
		while (*pos < end)
		{
			auto row = (Row*)(*pos);
			part_delete_by(part, tr, row);
			*pos += row_size(row);
		}
		break;
	}
	case CMD_DDL:
	{
		// replay ddl command
		catalog_execute(&storage->catalog, tr, *pos, 0);
		json_skip(pos);
		break;
	}
	default:
		error("recover: unrecognized command id: %d", cmd->cmd);
		break;
	}
}

static void
recover_next_record(Recover* self, Record* record)
{
	// replay transaction log record
	auto cmd = record_cmd(record);
	auto pos = record_data(record);
	for (auto i = record->count; i > 0; i--)
	{
		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate_cmd(cmd, pos)))
				error("recover: record command mismatch");
		recover_cmd(self, cmd, &pos);
		cmd++;
	}
	self->ops  += record->ops;
	self->size += record->size;

	// wal write, if necessary
	if (self->write_wal)
	{
		auto write = &self->write;
		auto write_list = &self->write_list;
		write_reset(write);
		write_begin(write);
		write_add(write, &self->tr.log.write_log);
		write_list_reset(write_list);
		write_list_add(write_list, write);
		wal_mgr_write(&self->storage->wal_mgr, write_list);
	} else
	{
		state_lsn_follow(record->lsn);
	}
}

void
recover_next(Recover* self, Record* record)
{
	auto tr = &self->tr;
	tr_reset(tr);
	auto on_error = error_catch
	(
		// begin
		tr_begin(tr);

		// replay
		recover_next_record(self, record);

		// commit
		tr_commit(tr);
	);
	if (unlikely(on_error))
	{
		tr_abort(tr);
		rethrow();
	}
}

static void
recover_wal_main(Recover* self)
{
	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	auto wal_mgr = &self->storage->wal_mgr;
	wal_mgr_start(wal_mgr);

	// replay wals starting from the last checkpoint
	auto id = state_checkpoint() + 1;
	for (;;)
	{
		recover_reset_stats(self);

		WalCursor cursor;
		wal_cursor_init(&cursor);
		defer(wal_cursor_close, &cursor);

		auto wal_crc = opt_int_of(&config()->wal_crc);
		wal_cursor_open(&cursor, &wal_mgr->wal, id, false, wal_crc);
		for (;;)
		{
			if (! wal_cursor_next(&cursor))
				break;
			auto record = wal_cursor_at(&cursor);
			recover_next(self, record);
		}
		if (! wal_cursor_active(&cursor))
			break;

		info(" wals/%" PRIu64 " (%.2f MiB, %" PRIu64 " rows)", id,
		     (double)self->size / 1024 / 1024,
		     self->ops);

		id = id_mgr_next(&wal_mgr->wal.list, cursor.file->id);
		if (id == UINT64_MAX)
			break;
	}
}

void
recover_wal(Recover* self)
{
	if (error_catch( recover_wal_main(self)) )
	{
		info("recover: wal replay error, last valid lsn is: %" PRIu64,
		     state_lsn());
		rethrow();
	}
}
