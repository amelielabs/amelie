
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
#include <sonata_db.h>

#if 0
hot static void
recover_log(Db* self, Transaction* trx, uint64_t lsn, uint8_t** pos)
{
	// [type, storage, data]
	int count;
	data_read_array(pos, &count);

	// type
	int64_t type;
	data_read_integer(pos, &type);

	// id_table
	Uuid id_table;
	Uuid id_storage;
	uuid_init(&id_table);
	uuid_init(&id_storage);
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		int64_t a, b;
		data_read_integer(pos, &a);
		data_read_integer(pos, &b);
		id_table.a = a;
		id_table.b = b;
		data_read_integer(pos, &a);
		data_read_integer(pos, &b);
		id_storage.a = a;
		id_storage.b = b;
	}

	// data
	uint8_t* data = *pos;
	data_skip(pos);

	// DML operations
	if (type == LOG_REPLACE || type == LOG_DELETE)
	{
		// find table by uuid
		auto table = table_mgr_find_by_id(&self->table_mgr, &id_table);
		if (unlikely(! table ))
			return;

		// find storage
		auto storage = table_mgr_find_storage(&self->table_mgr, id_storage);
		if (! storage)
			return;

		// todo: serial recover

		// todo: check last snapshot

		// replay write
		if (type == LOG_REPLACE)
			storage_set(storage, trx, false, &data);
		else
			storage_delete_by(storage, trx, &data);
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
		guard(schema_config_free, config);
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
		guard(table_config_free, config);
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
		guard(view_config_free, config);
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
	guard(transaction_free, &trx);

	Exception e;
	if (enter(&e))
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

	if (leave(&e))
	{
		log("recover: wal lsn %" PRIu64 ": replay error", lsn);
		transaction_abort(&trx);
		rethrow();
	}
}

#if 0
static void
recover_wal(Db* self)
{
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
	log("recover: begin wal replay");

	recover_wal(self);

	// start wal mgr
	wal_start(&self->wal);

	log("recover: complete");
}
#endif
#endif

void
recover(Db* self)
{
	// todo
	(void)self;
}

static inline int
snapshot_id_of(const char* name,
               uint64_t*   storage,
               uint64_t*   lsn,
               bool*       incomplete)
{
	// <storage_id>.<lsn>[.incomplete]
	*storage = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		*storage = (*storage * 10) + *name - '0';
		name++;
	}
	if (*name != '.')
		return -1;
	name++;
	*lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		*lsn = (*lsn * 10) + *name - '0';
		name++;
	}
	if (*name == '.')
	{
		if (! strcmp(name, ".incomplete"))
			*incomplete = true;
		return -1;
	}
	*incomplete = false;
	return 0;
}

static void
closedir_guard(DIR* self)
{
	closedir(self);
}

void
recover_basedir(Db* self)
{
	// read storage directory, get a list of snapshots per storage
	auto path = config_directory();
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("snapshot: directory '%s' open error", path);
	guard(closedir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		uint64_t id_storage;
		uint64_t id_lsn;
		bool     incomplete;
		if (snapshot_id_of(entry->d_name, &id_storage, &id_lsn, &incomplete) == -1)
			continue;
		if (incomplete)
		{
			log("snapshot: removing incomplete snapshot: '%s/%s'", path, entry->d_name);
			fs_unlink("%s/%s", path, entry->d_name);
			continue;
		}

		// todo: remove all snapshot files > config.snapshot

		auto storage = table_mgr_find_storage(&self->table_mgr, id_storage);
		if (storage == NULL)
		{
			log("snapshot: unknown storage for file: '%s/%s'", path, entry->d_name);
			continue;
		}
		snapshot_mgr_add(&storage->snapshot_mgr, id_lsn);
	}
}
