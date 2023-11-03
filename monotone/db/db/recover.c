
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

hot static void
recover_log(Db* self, Transaction* trx, uint64_t lsn, uint8_t** pos)
{
	// [type, uuid a/b, data]
	int count;
	data_read_array(pos, &count);

	// type
	int64_t type;
	data_read_integer(pos, &type);

	// uuid
	Uuid uuid;
	uuid_init(&uuid);
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		int64_t a, b;
		data_read_integer(pos, &a);
		data_read_integer(pos, &b);
		uuid.a = a;
		uuid.b = b;
	}

	// data
	int data_size;
	uint8_t* data = *pos;
	data_skip(pos);
	data_size = *pos - data;

	// DML operations
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		// find storage by uuid
		auto storage = storage_mgr_find(&self->storage_mgr, &uuid);
		if (unlikely(storage == NULL))
			return;

		// todo: serial recover

		// replay write
		if (type == LOG_REPLACE)
			storage_set(storage, trx, false, data, data_size);
		else
			storage_delete_by(storage, trx, data, data_size);
		return;
	}

	// do not apply ddl operations <= catalog checkpoint
	uint64_t catalog_snapshot;
	catalog_snapshot = var_int_of(&config()->catalog_snapshot);
	if (lsn <= catalog_snapshot)
		return;

	// DDL operations
	switch (type) {
	case LOG_SCHEMA_CREATE:
	{
		auto config = schema_op_create_read(&data);
		guard(config_guard, schema_config_free, config);
		schema_mgr_create(&self->schema_mgr, trx, config, false);
		break;
	}
	case LOG_SCHEMA_DROP:
	{
		Str name;
		schema_op_drop_read(&data, &name);
		schema_mgr_drop(&self->schema_mgr, trx, &name, true);
		break;
	}
	case LOG_SCHEMA_RENAME:
	{
		Str name;
		Str name_new;
		schema_op_rename_read(&data, &name, &name_new);
		schema_mgr_rename(&self->schema_mgr, trx, &name, &name_new, true);
		break;
	}
	case LOG_TABLE_CREATE:
	{
		auto config = table_op_create_read(&data);
		guard(config_guard, table_config_free, config);
		table_mgr_create(&self->table_mgr, trx, config, false);
		break;
	}
	case LOG_TABLE_DROP:
	{
		Str schema;
		Str name;
		table_op_drop_read(&data, &schema, &name);
		table_mgr_drop(&self->table_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_TABLE_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		table_op_rename_read(&data, &schema, &name, &schema_new, &name_new);
		table_mgr_rename(&self->table_mgr, trx, &schema, &name,
		                 &schema_new, &name_new, true);
		break;
	}
	case LOG_VIEW_CREATE:
	{
		auto config = view_op_create_read(&data);
		guard(config_guard, view_config_free, config);
		view_mgr_create(&self->view_mgr, trx, config, false);
		break;
	}
	case LOG_VIEW_DROP:
	{
		Str schema;
		Str name;
		view_op_drop_read(&data, &schema, &name);
		view_mgr_drop(&self->view_mgr, trx, &schema, &name, true);
		break;
	}
	case LOG_VIEW_RENAME:
	{
		Str schema;
		Str name;
		Str schema_new;
		Str name_new;
		view_op_rename_read(&data, &schema, &name, &schema_new, &name_new);
		view_mgr_rename(&self->view_mgr, trx, &schema, &name,
		                &schema_new, &name_new, true);
		break;
	}
	default:
		error("recover: unrecognized operation id: %d", type);
		break;
	}
}

void
recover_write(Db* self, uint64_t lsn, uint8_t* pos, bool write_wal)
{
	Transaction trx;
	transaction_init(&trx);
	guard(trx_guard, transaction_free, &trx);

	Exception e;
	if (try(&e))
	{
		// begin
		transaction_begin(&trx);
		transaction_set_auto_commit(&trx);

		// replay transaction log
		int count;
		data_read_array(&pos, &count);
		for (int i = 0; i < count; i++)
			recover_log(self, &trx, lsn, &pos);

		// todo: wal write
		unused(write_wal);

		config_lsn_follow(lsn);

		// assign lsn
		transaction_set_lsn(&trx, lsn);

		// commit
		transaction_commit(&trx);
	}

	if (catch(&e))
	{
		log("recover: wal lsn %" PRIu64 ": replay error", lsn);
		transaction_abort(&trx);
		rethrow();
	}
}

static void
recover_wal(Db* self)
{
	log("recover: begin wal replay");

	WalCursor cursor;
	wal_cursor_init(&cursor);
	guard(cursor_guard, wal_cursor_close, &cursor);

	wal_cursor_open(&cursor, &self->wal, 0);
	for (uint64_t total = 0 ;;)
	{
		auto buf = wal_cursor_next(&cursor);
		if (unlikely(buf == NULL))
			break;
		guard(buf_guard, buf_free, buf);

		// WAL_WRITE
		auto msg = msg_of(buf);
		if (msg->id != MSG_WAL_WRITE)
			error("recover: unrecognized operation: %d", msg->id);

		// [lsn, log]
		uint8_t* pos = msg_of(buf)->data;
		int count;
		data_read_array(&pos, &count);
		int64_t lsn;
		data_read_integer(&pos, &lsn);

		// replay log
		recover_write(self, lsn, pos, false);

		total++;
		if ((total % 100000) == 0)
			log("recover: %.1f million records processed",
			    total / 1000000.0);
	}
}

void
recover(Db* self)
{
	// prepare wal mgr
	wal_open(&self->wal);

	// replay logs
	recover_wal(self);

	// start wal mgr
	wal_start(&self->wal);
}
