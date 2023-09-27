#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct WalRecord WalRecord;

struct WalRecord
{
	int  count;
	Buf  data;
	Iov  iov;
	List link;
};

static inline void
wal_record_init(WalRecord* self)
{
	self->count = 0;
	buf_init(&self->data);
	iov_init(&self->iov);
	list_init(&self->link);
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
	self->count = 0;
	buf_reset(&self->data);
	iov_reset(&self->iov);
}

hot static inline void
wal_record_create(WalRecord* self, Log* log)
{
	// write log records without array
	int current = 0;
	for (; current < log->count; current++)
	{
		// [cmd, uuid, data]
		auto op = log_of(log, current);
		switch (op->type) {
		case LOG_WRITE:
		{
			// write only primary index updates
			auto iface = op->write.iface;
			auto iface_arg = op->write.iface_arg;
			if (! iface->is_primary(iface_arg))
				continue;

			// get storage uuid
			auto uuid = iface->uuid(iface_arg);
			char uuid_sz[UUID_SZ];
			uuid_to_string(uuid, uuid_sz, sizeof(uuid_sz));

			// [cmd, uuid, data]
			int data_start = buf_size(&self->data);
			encode_array(&self->data, 3);
			encode_integer(&self->data, op->cmd);
			encode_raw(&self->data, uuid_sz, sizeof(uuid_sz) - 1);
			int data_size = buf_size(&self->data) - data_start;
			iov_add_buf(&self->iov, &self->data, data_start, data_size);

			// row
			auto schema = iface->schema(iface_arg);
			auto row = op->write.row;
			iov_add(&self->iov, row_data(row, schema), row_data_size(row, schema));
			break;
		}
		case LOG_WRITE_HANDLE:
		{
			// [cmd, null, op]
			int data_start = buf_size(&self->data);
			encode_array(&self->data, 3);
			encode_integer(&self->data, op->cmd);
			encode_null(&self->data);
			int data_size = buf_size(&self->data) - data_start;
			iov_add_buf(&self->iov, &self->data, data_start, data_size);

			// op
			iov_add(&self->iov, op->write_handle.data->start, buf_size(op->write_handle.data));
			break;
		}
		}

		self->count++;
	}
}
