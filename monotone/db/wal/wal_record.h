#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct WalRecord WalRecord;

struct WalRecord
{
	Buf data;
	Iov iov;
};

static inline void
wal_record_init(WalRecord* self)
{
	buf_init(&self->data);
	iov_init(&self->iov);
}

static inline void
wal_record_free(WalRecord* self)
{
	buf_free(&self->data);
	iov_free(&self->iov);
}

static inline void
wal_record_reset(WalRecord* self)
{
	buf_reset(&self->data);
	iov_reset(&self->iov);
}

hot static inline void
wal_record_create(WalRecord* self, Log* log, uint64_t lsn)
{
	wal_record_reset(self);

	// msg
	int start = buf_size(&self->data);
	int size  = 0;
	buf_reserve(&self->data, sizeof(Msg));

	auto msg = (Msg*)(self->data.start + start);
	msg->id   = MSG_WAL_WRITE;
	msg->size = 0;
	buf_advance(&self->data, sizeof(Msg));

	// [lsn, [log]]
	encode_array(&self->data, 2);
	encode_integer(&self->data, lsn);
	encode_array(&self->data, log->count);

	size += buf_size(&self->data) - start;
	iov_add_buf(&self->iov, &self->data, start, size);

	int current = 0;
	for (; current < log->count; current++)
	{
		// [cmd, uuid, data]
		auto op = log_of(log, current);
		switch (op->type) {
		case LOG_WRITE:
		{
			Heap* heap = op->write.heap;
			Row* row = op->write.row;

			char uuid[UUID_SZ];
			uuid_to_string(heap->uuid, uuid, sizeof(uuid));

			int data_start = buf_size(&self->data);
			encode_array(&self->data, 3);
			encode_integer(&self->data, op->cmd);
			encode_raw(&self->data, uuid, sizeof(uuid) - 1);

			int data_size = buf_size(&self->data) - data_start;
			iov_add_buf(&self->iov, &self->data, data_start, data_size);
			iov_add(&self->iov, row_of(row, heap->schema), row_data_size(row));
			size += data_size + row_data_size(row);
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			int data_start = buf_size(&self->data);
			encode_array(&self->data, 3);
			encode_integer(&self->data, op->cmd);
			encode_null(&self->data);

			int data_size  = buf_size(&self->data) - data_start;
			iov_add_buf(&self->iov, &self->data, data_start, data_size);
			iov_add(&self->iov, op->write_handle.data->start, buf_size(op->write_handle.data));
			size += data_size + buf_size(op->write_handle.data);
			break;
		}
		}
	}

	// create iov list
	iov_prepare(&self->iov);

	// update message size
	msg = (Msg*)(self->data.start + start);
	msg->size = size;
}
