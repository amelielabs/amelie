
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
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_wal.h>

Buf*
wal_debug_list_files(Wal* self)
{
	unused(self);
	auto buf = msg_create(MSG_OBJECT);

	int array_offset = buf_size(buf);
	encode_array32(buf, 0);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/wal", config_directory());

	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("engine: directory '%s' open error", path);
	guard(dir_guard, closedir, dir);

	int count = 0;
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		encode_raw(buf, entry->d_name, strlen(entry->d_name));
		count++;
	}

	uint8_t* pos = buf->start + array_offset;
	data_write_array32(&pos, count);

	msg_end(buf);
	return buf;
}

Buf*
wal_debug_flush(Wal* self)
{
	auto buf = msg_create(MSG_OBJECT);
	encode_array(buf, 3);
	encode_integer(buf, config_lsn());
	encode_integer(buf, self->wal_store.current->id);
	encode_integer(buf, self->wal_store.current->file.size);
	msg_end(buf);
	return buf;
}

Buf*
wal_debug_gc(Wal* self, uint64_t snapshot)
{
	wal_gc(self, snapshot);
	auto buf = msg_create(MSG_OBJECT);
	encode_bool(buf, true);
	msg_end(buf);
	return buf;
}

Buf*
wal_debug_read(Wal* self, uint64_t lsn)
{
	auto buf = msg_create(MSG_OBJECT);

	WalCursor cursor;
	wal_cursor_init(&cursor);
	guard(cursor_guard, wal_cursor_close, &cursor);

	bool found = false;
	wal_cursor_open(&cursor, self, lsn);
	for (;;)
	{
		auto record = wal_cursor_next(&cursor);
		if (unlikely(record == NULL))
			break;

		auto msg = msg_of(record);
		uint8_t* pos = msg->data;
		int count;
		data_read_array(&pos, &count);
		int64_t record_lsn;
		data_read_integer(&pos, &record_lsn);

		if (record_lsn == (int64_t)lsn)
		{
			buf_write(buf, msg->data, msg_data_size(msg));
			buf_free(record);
			found = true;
			break;
		}
		buf_free(record);
	}

	if (! found)
		encode_null(buf);

	msg_end(buf);
	return buf;
}

Buf*
wal_debug_snapshot(Wal* self, uint64_t snapshot)
{
	snapshot_mgr_add(&self->wal_store.snapshot, snapshot);

	auto buf = msg_create(MSG_OBJECT);
	encode_bool(buf, true);
	msg_end(buf);
	return buf;
}

Buf*
wal_debug_snapshot_del(Wal* self, uint64_t snapshot)
{
	snapshot_mgr_delete(&self->wal_store.snapshot, snapshot);

	auto buf = msg_create(MSG_OBJECT);
	encode_bool(buf, true);
	msg_end(buf);
	return buf;
}
