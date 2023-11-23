#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SnapshotBatch SnapshotBatch;

struct SnapshotBatch
{
	int count;
	int count_batch;
	Iov iov;
	Buf data;
};

static inline void
snapshot_batch_init(SnapshotBatch* self)
{
	self->count = 0;
	self->count_batch = 0;
	iov_init(&self->iov);
	buf_init(&self->data);
}

static inline void
snapshot_batch_free(SnapshotBatch* self)
{
	iov_free(&self->iov);
	buf_free(&self->data);
}

static inline void
snapshot_batch_reset(SnapshotBatch* self)
{
	self->count = 0;
	iov_reset(&self->iov);
	buf_reset(&self->data);
}

static inline void
snapshot_batch_create(SnapshotBatch* self, int count_batch)
{
	self->count_batch = count_batch;
	int size_storage_msg;
	size_storage_msg = sizeof(Msg) + sizeof(Uuid);
	buf_reserve(&self->data, size_storage_msg + sizeof(Msg) * count_batch);
	iov_reserve(&self->iov, count_batch);
}

static inline bool
snapshot_batch_has(SnapshotBatch* self)
{
	return self->count > 0;
}

static inline bool
snapshot_batch_full(SnapshotBatch* self)
{
	return self->count == self->count_batch;
}

static inline void
snapshot_batch_add_storage(SnapshotBatch* self, Uuid* id)
{
	// MSG_SNAPSHOT_STORAGE
	auto msg = (Msg*)self->data.position;
	int  msg_size = sizeof(Msg) + sizeof(Uuid);
	buf_advance(&self->data, msg_size);
	msg->size = msg_size;
	msg->id   = MSG_SNAPSHOT_STORAGE;
	memcpy(msg->data, id, sizeof(Uuid));
	iov_add(&self->iov, msg, msg_size);
	self->count++;
}

static inline void
snapshot_batch_add(SnapshotBatch* self, Def* def, Row* row)
{
	assert(self->count < self->count_batch);

	// MSG_SNAPSHOT_ROW
	auto msg = (Msg*)self->data.position;
	buf_advance(&self->data, sizeof(Msg));
	msg->size = 0;
	msg->id   = MSG_SNAPSHOT_ROW;
	iov_add(&self->iov, msg, sizeof(Msg));
	self->count++;

	// row data
	uint8_t* data = row_data(row, def);
	int      data_size = row_data_size(row, def);
	iov_add(&self->iov, data, data_size);
	msg->size = sizeof(Msg) + data_size;
}
