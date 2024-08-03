
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
recover_init(Recover*   self, Db* db,
             RecoverIf* iface,
             void*      iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->db        = db;
	transaction_init(&self->trx);
	wal_batch_init(&self->batch);
}

void
recover_free(Recover* self)
{
	transaction_free(&self->trx);
	wal_batch_free(&self->batch);
}

hot void
recover_next(Recover* self, uint8_t** meta, uint8_t** data)
{
	auto db  = self->db;
	auto trx = &self->trx;

	// [dml, partition]
	// [ddl]

	// type
	int64_t type;
	data_read_integer(meta, &type);

	// DML operations
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		int64_t partition_id;
		data_read_integer(meta, &partition_id);

		// find partition by id
		auto part = table_mgr_find_partition(&db->table_mgr, partition_id);
		if (! part)
			error("failed to find partition %" PRIu64, partition_id);

		// todo: serial recover

		// replay write
		if (type == LOG_REPLACE)
			part_insert(part, trx, true, data);
		else
			part_delete_by(part, trx, data);
		return;
	}

	// DDL operations
	switch (type) {
	case LOG_SCHEMA_CREATE:
	{
		auto config = schema_op_create_read(data);
		guard(schema_config_free, config);
		schema_mgr_create(&db->schema_mgr, trx, config, false);
		break;
	}
	case LOG_SCHEMA_DROP:
	{
		Str name;
		schema_op_drop_read(data, &name);
		schema_mgr_drop(&db->schema_mgr, trx, &name, true);
		break;
	}
	case LOG_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		schema_op_rename_read(data, &name, &name_new);
		schema_mgr_rename(&db->schema_mgr, trx, &name, &name_new, true);
		break;
	}
	case LOG_TABLE_CREATE:
	{
		auto config = table_op_create_read(data);
		guard(table_config_free, config);
		table_mgr_create(&db->table_mgr, trx, config, false);
		break;
	}
	case LOG_TABLE_DROP:
	{
		Str schema;
		Str name;
		table_op_drop_read(data, &schema, &name);
		table_mgr_drop(&db->table_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		table_op_rename_read(data, &schema, &name, &schema_new, &name_new);
		table_mgr_rename(&db->table_mgr, trx, &schema, &name,
		                 &schema_new, &name_new, true);
		break;
	}
	case LOG_TABLE_TRUNCATE:
	{
		Str schema;
		Str name;
		table_op_truncate_read(data, &schema, &name);
		table_mgr_truncate(&db->table_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_TABLE_COLUMN_RENAME:
	{
		Str schema;
		Str name;
		Str name_column;
		Str name_column_new;
		table_op_column_rename_read(data, &schema, &name, &name_column, &name_column_new);
		table_mgr_column_rename(&db->table_mgr, trx, &schema, &name,
		                        &name_column,
		                        &name_column_new, true);
		break;
	}
	case LOG_TABLE_COLUMN_ADD:
	{
		Str schema;
		Str name;
		auto column = table_op_column_add_read(data, &schema, &name);
		guard(column_free, column);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_add(&db->table_mgr, trx, &schema, &name,
		                                      column, true);
		// build new table with new column
		self->iface->build_column_add(self, table, table_new, column);
		break;
	}
	case LOG_TABLE_COLUMN_DROP:
	{
		Str schema;
		Str name;
		Str name_column;
		table_op_column_drop_read(data, &schema, &name, &name_column);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto table_new = table_mgr_column_drop(&db->table_mgr, trx, &schema, &name,
		                                       &name_column, true);
		auto column = columns_find(&table->config->columns, &name_column);
		assert(column);
		// build new table without column
		self->iface->build_column_drop(self, table, table_new, column);
		break;
	}
	case LOG_INDEX_CREATE:
	{
		Str schema;
		Str name;
		auto config_pos = table_op_create_index_read(data, &schema, &name);
		auto table = table_mgr_find(&db->table_mgr, &schema, &name, true);
		auto config = index_config_read(table_columns(table), &config_pos);
		guard(index_config_free, config);
		table_index_create(table, trx, config, false);
		// build index
		auto index = table_find_index(table, &config->name, true);
		self->iface->build_index(self, table, index);
		break;
	}
	case LOG_INDEX_DROP:
	{
		Str table_schema;
		Str table_name;
		Str name;
		table_op_drop_index_read(data, &table_schema, &table_name, &name);
		auto table = table_mgr_find(&db->table_mgr, &table_schema, &table_name, true);
		table_index_drop(table, trx, &name, false);
		break;
	}
	case LOG_INDEX_RENAME:
	{
		Str table_schema;
		Str table_name;
		Str name;
		Str name_new;
		table_op_rename_index_read(data, &table_schema, &table_name, &name, &name_new);
		auto table = table_mgr_find(&db->table_mgr, &table_schema, &table_name, true);
		table_index_rename(table, trx, &name, &name_new, false);
		break;
	}
	case LOG_VIEW_CREATE:
	{
		auto config = view_op_create_read(data);
		guard(view_config_free, config);
		view_mgr_create(&db->view_mgr, trx, config, false);
		break;
	}
	case LOG_VIEW_DROP:
	{
		Str schema;
		Str name;
		view_op_drop_read(data, &schema, &name);
		view_mgr_drop(&db->view_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_VIEW_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		view_op_rename_read(data, &schema, &name, &schema_new, &name_new);
		view_mgr_rename(&db->view_mgr, trx, &schema, &name,
		                &schema_new, &name_new, true);
		break;
	}
	default:
		error("recover: unrecognized operation id: %d", type);
		break;
	}
}

void
recover_next_write(Recover* self, WalWrite* write, bool write_wal, int flags)
{
	auto trx = &self->trx;
	transaction_reset(trx);

	Exception e;
	if (enter(&e))
	{
		// begin
		transaction_begin(trx);

		// replay transaction log

		// [header][meta][data]
		auto meta = (uint8_t*)write + sizeof(*write);
		auto data = meta + write->offset;
		for (auto i = write->count; i > 0; i--)
			recover_next(self, &meta, &data);

		// wal write, if necessary
		if (write_wal)
		{
			auto batch = &self->batch;
			wal_batch_reset(batch);
			wal_batch_begin(batch, flags);
			wal_batch_add(batch, &trx->log.log_set);
			wal_write(&self->db->wal, batch);
		} else
		{
			config_lsn_follow(write->lsn);
		}

		// commit
		transaction_commit(trx);
	}

	if (leave(&e))
	{
		info("recover: wal lsn %" PRIu64 ": replay error", write->lsn);
		transaction_abort(trx);
		rethrow();
	}
}

void
recover_wal(Recover* self)
{
	if (! var_int_of(&config()->wal))
		return;

	// prepare wal mgr
	auto wal = &self->db->wal;
	wal_open(wal);

	// start wal recover from the last checkpoint
	auto checkpoint = config_checkpoint() + 1;
	info("recover: wal from: %" PRIu64, checkpoint);

	WalCursor cursor;
	wal_cursor_init(&cursor);
	guard(wal_cursor_close, &cursor);

	int64_t total = 0;
	int64_t total_writes = 0;
	wal_cursor_open(&cursor, wal, checkpoint);
	for (;;)
	{
		if (! wal_cursor_next(&cursor))
			break;
		auto write = wal_cursor_at(&cursor);
		recover_next_write(self, write, false, 0);

		total_writes += write->count;
		total++;
		if ((total % 10000) == 0)
			info("recover: %.1f million records processed",
			     total_writes / 1000000.0);
	}

	info("recover: complete (%" PRIu64 " writes)", total_writes);
}
