
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
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
recover_init(Recover*   self, Db* db,
             bool       write_wal,
             RecoverIf* iface,
             void*      iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
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

hot static void
recover_cmd(Recover* self, RecordCmd* cmd, uint8_t** pos)
{
	auto db = self->db;
	auto tr = &self->tr;
	switch (cmd->cmd) {
	case CMD_REPLACE:
	{
		// find partition by id
		auto part = part_mgr_find(&db->part_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu64, cmd->partition);
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
		auto part = part_mgr_find(&db->part_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu64, cmd->partition);
		auto end = *pos + cmd->size;
		while (*pos < end)
		{
			auto row = (Row*)(*pos);
			part_delete_by(part, tr, row);
			*pos += row_size(row);
		}
		break;
	}
	case CMD_SCHEMA_CREATE:
	{
		auto config = schema_op_create_read(pos);
		defer(schema_config_free, config);
		schema_mgr_create(&db->schema_mgr, tr, config, false);
		break;
	}
	case CMD_SCHEMA_DROP:
	{
		Str name;
		schema_op_drop_read(pos, &name);
		schema_mgr_drop(&db->schema_mgr, tr, &name, true);
		break;
	}
	case CMD_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		schema_op_rename_read(pos, &name, &name_new);
		schema_mgr_rename(&db->schema_mgr, tr, &name, &name_new, true);
		break;
	}
	case CMD_TABLE_CREATE:
	{
		auto config = table_op_create_read(pos);
		defer(table_config_free, config);
		table_mgr_create(&db->table_mgr, tr, config, false);
		break;
	}
	case CMD_TABLE_DROP:
	{
		Str schema;
		Str name;
		table_op_drop_read(pos, &schema, &name);
		table_mgr_drop(&db->table_mgr, tr, &schema, &name, true);
		break;
	}
	case CMD_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		table_op_rename_read(pos, &schema, &name, &schema_new, &name_new);
		table_mgr_rename(&db->table_mgr, tr, &schema, &name,
		                 &schema_new, &name_new, true);
		break;
	}
	case CMD_TABLE_SET_UNLOGGED:
	{
		Str  schema;
		Str  name;
		bool value;
		table_op_set_unlogged_read(pos, &schema, &name, &value);
		table_mgr_set_unlogged(&db->table_mgr, tr, &schema, &name, value, true);
		break;
	}
	case CMD_TABLE_TRUNCATE:
	{
		Str schema;
		Str name;
		table_op_truncate_read(pos, &schema, &name);
		table_mgr_truncate(&db->table_mgr, tr, &schema, &name, true);
		break;
	}
	case CMD_TABLE_COLUMN_RENAME:
	{
		Str schema;
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_rename_read(pos, &schema, &name, &name_column, &name_column_new);
		table_mgr_column_rename(&db->table_mgr, tr, &schema, &name,
		                        &name_column,
		                        &name_column_new, true);
		break;
	}
	case CMD_TABLE_COLUMN_ADD:
	{
		Str schema;
		Str name;
		auto column = table_op_column_add_read(pos, &schema, &name);
		defer(column_free, column);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_add(&db->table_mgr, tr, &schema, &name,
		                                      column, true);
		if (! table_new)
			break;
		// build new table with new column
		self->iface->build_column_add(self, table, table_new, column);
		break;
	}
	case CMD_TABLE_COLUMN_DROP:
	{
		Str schema;
		Str name;
		Str name_column;
		table_op_column_drop_read(pos, &schema, &name, &name_column);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_drop(&db->table_mgr, tr, &schema, &name,
		                                       &name_column, true);
		if (! table_new)
			break;
		auto column = columns_find(&table->config->columns, &name_column);
		assert(column);
		// build new table without column
		self->iface->build_column_drop(self, table, table_new, column);
		break;
	}
	case CMD_TABLE_COLUMN_SET_DEFAULT:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		table_op_column_set_read(pos, &schema, &name, &name_column,
		                         &value_prev, &value);
		table_mgr_column_set_default(&db->table_mgr, tr, &schema, &name,
		                             &name_column, &value, true);
		break;
	}
	case CMD_TABLE_COLUMN_SET_IDENTITY:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		table_op_column_set_read(pos, &schema, &name, &name_column,
		                         &value_prev, &value);
		table_mgr_column_set_identity(&db->table_mgr, tr, &schema, &name,
		                              &name_column, &value, true);
		break;
	}
	case CMD_TABLE_COLUMN_SET_STORED:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		table_op_column_set_read(pos, &schema, &name, &name_column,
		                         &value_prev, &value);
		table_mgr_column_set_stored(&db->table_mgr, tr, &schema, &name,
		                            &name_column, &value, true);
		break;
	}
	case CMD_TABLE_COLUMN_SET_RESOLVED:
	{
		Str schema;
		Str name;
		Str name_column;
		Str value_prev;
		Str value;
		table_op_column_set_read(pos, &schema, &name, &name_column,
		                         &value_prev, &value);
		table_mgr_column_set_resolved(&db->table_mgr, tr, &schema, &name,
		                              &name_column, &value, true);
		break;
	}
	case CMD_INDEX_CREATE:
	{
		Str schema;
		Str name;
		auto config_pos = table_op_create_index_read(pos, &schema, &name);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto config = index_config_read(table_columns(table), &config_pos);
		defer(index_config_free, config);
		table_index_create(table, tr, config, false);
		// build index
		auto index = table_find_index(table, &config->name, true);
		self->iface->build_index(self, table, index);
		break;
	}
	case CMD_INDEX_DROP:
	{
		Str table_schema;
		Str table_name;
		Str name;
		table_op_drop_index_read(pos, &table_schema, &table_name, &name);
		auto table = table_mgr_find(&db->table_mgr, &table_schema, &table_name, true);
		table_index_drop(table, tr, &name, false);
		break;
	}
	case CMD_INDEX_RENAME:
	{
		Str table_schema;
		Str table_name;
		Str name;
		Str name_new;
		table_op_rename_index_read(pos, &table_schema, &table_name, &name, &name_new);
		auto table = table_mgr_find(&db->table_mgr, &table_schema, &table_name, true);
		table_index_rename(table, tr, &name, &name_new, false);
		break;
	}
	case CMD_PROC_CREATE:
	{
		auto config = proc_op_create_read(pos);
		defer(proc_config_free, config);
		proc_mgr_create(&db->proc_mgr, tr, config, false);
		break;
	}
	case CMD_PROC_DROP:
	{
		Str schema;
		Str name;
		proc_op_drop_read(pos, &schema, &name);
		proc_mgr_drop(&db->proc_mgr, tr, &schema, &name, true);
		break;
	}
	case CMD_PROC_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		proc_op_rename_read(pos, &schema, &name, &schema_new, &name_new);
		proc_mgr_rename(&db->proc_mgr, tr, &schema, &name,
		                &schema_new, &name_new, true);
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
		wal_mgr_write(&self->db->wal_mgr, write_list);
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
	auto wal_mgr = &self->db->wal_mgr;
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

		info("wals/%" PRIu64 " (%.2f MiB, %" PRIu64 " rows)",
		     id, (double)self->size / 1024 / 1024, self->ops);

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
