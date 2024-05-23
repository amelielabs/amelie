
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>

hot static void
recover_partition(Part* self, Table* table)
{
	auto checkpoint = config_checkpoint();
	log("recover %" PRIu64 ": %.*s.%.*s (partition %" PRIu64 ")",
	    checkpoint,
	    str_size(&table->config->schema),
	    str_of(&table->config->schema),
	    str_size(&table->config->name),
	    str_of(&table->config->name),
	    self->config->id);

	SnapshotCursor cursor;
	snapshot_cursor_init(&cursor);
	guard(snapshot_cursor_close, &cursor);

	snapshot_cursor_open(&cursor, checkpoint, self->config->id);
	uint64_t count = 0;
	for (;;)
	{
		auto buf = snapshot_cursor_next(&cursor);
		if (! buf)
			break;
		guard_buf(buf);
		auto pos = msg_of(buf)->data;
		part_ingest(self, &pos);
		count++;
	}

	log("recover %" PRIu64 ": %.*s.%.*s (partition %" PRIu64 ") %" PRIu64 " rows loaded",
	    checkpoint,
	    str_size(&table->config->schema),
	    str_of(&table->config->schema),
	    str_size(&table->config->name),
	    str_of(&table->config->name),
	    self->config->id,
	    count);
}

hot void
recover(Db* self, Uuid* shard)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		list_foreach(&table->part_mgr.list)
		{
			auto part = list_at(Part, link);
			if (! uuid_compare(&part->config->shard, shard))
				continue;
			recover_partition(part, table);
		}
	}
}

hot static void
recover_cmd(Db* self, Transaction* trx, uint8_t** meta, uint8_t** data)
{
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
		auto part = table_mgr_find_partition(&self->table_mgr, partition_id);
		if (! part)
			error("failed to find partition %" PRIu64, partition_id);

		// todo: serial recover

		// replay write
		if (type == LOG_REPLACE)
			part_set(part, trx, false, data);
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
		schema_mgr_create(&self->schema_mgr, trx, config, false);
		break;
	}
	case LOG_SCHEMA_DROP:
	{
		Str name;
		schema_op_drop_read(data, &name);
		schema_mgr_drop(&self->schema_mgr, trx, &name, true);
		break;
	}
	case LOG_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		schema_op_rename_read(data, &name, &name_new);
		schema_mgr_rename(&self->schema_mgr, trx, &name, &name_new, true);
		break;
	}
	case LOG_TABLE_CREATE:
	{
		auto config = table_op_create_read(data);
		guard(table_config_free, config);
		table_mgr_create(&self->table_mgr, trx, config, false);
		break;
	}
	case LOG_TABLE_DROP:
	{
		Str schema;
		Str name;
		table_op_drop_read(data, &schema, &name);
		table_mgr_drop(&self->table_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		table_op_rename_read(data, &schema, &name, &schema_new, &name_new);
		table_mgr_rename(&self->table_mgr, trx, &schema, &name,
		                 &schema_new, &name_new, true);
		break;
	}
	case LOG_VIEW_CREATE:
	{
		auto config = view_op_create_read(data);
		guard(view_config_free, config);
		view_mgr_create(&self->view_mgr, trx, config, false);
		break;
	}
	case LOG_VIEW_DROP:
	{
		Str schema;
		Str name;
		view_op_drop_read(data, &schema, &name);
		view_mgr_drop(&self->view_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_VIEW_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		view_op_rename_read(data, &schema, &name, &schema_new, &name_new);
		view_mgr_rename(&self->view_mgr, trx, &schema, &name,
		                &schema_new, &name_new, true);
		break;
	}
	default:
		error("recover: unrecognized operation id: %d", type);
		break;
	}
}

static void
recover_log(Db* self, WalWrite* write)
{
	Transaction trx;
	transaction_init(&trx);
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
	{
		// begin
		transaction_begin(&trx);

		// replay transaction log

		// [header][meta][data]
		auto meta = (uint8_t*)write + sizeof(*write);
		auto data = meta + write->offset;
		for (auto i = write->count; i > 0; i--)
			recover_cmd(self, &trx, &meta, &data);

		// todo: wal write

		// commit
		transaction_commit(&trx);

		config_lsn_follow(write->lsn);
	}

	if (leave(&e))
	{
		log("recover: wal lsn %" PRIu64 ": replay error", write->lsn);
		transaction_abort(&trx);
		rethrow();
	}
}

void
recover_wal(Db* self)
{
	if (! var_int_of(&config()->wal))
		return;

	// prepare wal mgr
	wal_open(&self->wal);

	// start wal recover from the last checkpoint
	auto checkpoint = config_checkpoint() + 1;
	log("recover: wal from: %" PRIu64, checkpoint);

	WalCursor cursor;
	wal_cursor_init(&cursor);
	guard(wal_cursor_close, &cursor);

	int64_t total = 0;
	int64_t total_writes = 0;
	wal_cursor_open(&cursor, &self->wal, checkpoint);
	for (;;)
	{
		if (! wal_cursor_next(&cursor))
			break;
		auto header = wal_cursor_at(&cursor);
		recover_log(self, header);

		total_writes += header->count;
		total++;
		if ((total % 10000) == 0)
			log("recover: %.1f million records processed",
			    total_writes / 1000000.0);
	}

	log("recover: complete (%" PRIu64 " writes)", total_writes);
}
