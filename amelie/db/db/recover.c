
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_tier.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>
#include <amelie_db.h>

void
recover_init(Recover* self, Db* db, bool write_wal)
{
	self->ops       = 0;
	self->size      = 0;
	self->write_wal = write_wal;
	self->db        = db;
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

static inline Part*
recover_map(Recover* self, Record* record, RecordCmd* cmd, Row* row)
{
	// find table by id and map partition
	auto db = self->db;
	auto table = table_mgr_find_by(&db->catalog.table_mgr, &cmd->id, false);
	if  (! table)
		return NULL;
	auto part = part_mapping_map(&table->part_mgr.mapping, row);
	if (! part)
		error("recover: partition mapping failed");

	// update track lsn
	track_lsn_follow(&part->track, record->lsn);

	// skip partition if it is already includes lsn
	if (record->lsn <= part->heap->header->lsn)
		part = NULL;
	return part;
}

hot static void
recover_cmd(Recover* self, Record* record, RecordCmd* cmd, uint8_t** pos)
{
	auto db = self->db;
	auto tr = &self->tr;
	switch (cmd->cmd) {
	case CMD_REPLACE:
	{
		// map partition
		auto part = recover_map(self, record, cmd, (Row*)*pos);
		if (! part)
		{
			record_cmd_skip(cmd, pos);
			break;
		}
		auto end = *pos + cmd->size;
		while (*pos < end)
		{
			auto row = row_copy(part->heap, (Row*)*pos);
			part_insert(part, tr, true, row);
			*pos += row_size(row);
		}
		break;
	}
	case CMD_DELETE:
	{
		// map partition
		auto part = recover_map(self, record, cmd, (Row*)*pos);
		if (! part)
		{
			record_cmd_skip(cmd, pos);
			break;
		}
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
	case CMD_DDL_CREATE_INDEX:
	{
		// skip ddl commands before the last catalog lsn
		if (record->lsn <= state_catalog())
		{
			record_cmd_skip(cmd, pos);
			break;
		}

		// execute ddl command
		if (cmd->cmd == CMD_DDL_CREATE_INDEX)
			service_create_index(&db->service, tr, *pos, 0);
		else
			catalog_execute(&db->catalog, tr, *pos, 0);
		json_skip(pos);

		// update catalog pending lsn
		opt_int_set(&state()->catalog_pending, record->lsn);
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
		recover_cmd(self, record, cmd, &pos);
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
		wal_mgr_write(&self->db->wal_mgr, write_list);
	} else
	{
		state_lsn_follow(record->lsn);
	}

	// unlock catalog after create index
	unlock_all();
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
	auto wal_mgr = &self->db->wal_mgr;
	wal_mgr_start(wal_mgr);

	// replay all wals
	uint64_t id = 0;
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

		info("recover: wal/%" PRIu64 " (%.2f MiB, %" PRIu64 " rows)", cursor.file->id,
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
