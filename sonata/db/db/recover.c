
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_wal.h>
#include <sonata_db.h>

hot static void
recover_cmd(Db* self, Transaction* trx, uint8_t** meta, uint8_t** data)
{
	// [dml, storage]
	// [ddl]

	// type
	int64_t type;
	data_read_integer(meta, &type);

	// DML operations
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		int64_t storage_id;
		data_read_integer(meta, &storage_id);

		// find storage by id
		auto storage = table_mgr_find_storage(&self->table_mgr, storage_id);
		if (! storage)
			error("");

		// todo: serial recover

		// replay write
		if (type == LOG_REPLACE)
			storage_set(storage, trx, false, data);
		else
			storage_delete_by(storage, trx, data);
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
		transaction_set_auto_commit(&trx);

		// replay transaction log

		// [header][meta][data]
		auto meta = (uint8_t*)write + sizeof(*write);
		auto data = meta + write->offset;
		for (auto i = write->count; i > 0; i--)
			recover_cmd(self, &trx, &meta, &data);

		// todo: wal write

		// commit
		transaction_set_lsn(&trx, write->lsn);
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

static void
recover_wal(Db* self)
{
	WalCursor cursor;
	wal_cursor_init(&cursor);
	guard(wal_cursor_close, &cursor);

	wal_cursor_open(&cursor, &self->wal, 0);
	for (uint64_t total = 0;;)
	{
		if (! wal_cursor_next(&cursor))
			break;
		auto header = wal_cursor_at(&cursor);
		recover_log(self, header);

		total += header->count;
		if ((total % 100000) == 0)
			log("recover: %.1f million records processed",
			    total / 1000000.0);
	}
}

void
recover(Db* self)
{
	if (! var_int_of(&config()->wal))
		return;

	// prepare wal mgr
	wal_open(&self->wal);

	// replay logs
	log("recover: begin wal replay");

	recover_wal(self);

	log("recover: complete");
}
