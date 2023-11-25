#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct SnapshotWriter SnapshotWriter;

struct SnapshotWriter
{
	int count;
	int count_max;
	Iov iov;
	Buf data;
};

static inline void
snapshot_writer_init(SnapshotWriter* self)
{
	self->count = 0;
	self->count_max = 0;
	iov_init(&self->iov);
	buf_init(&self->data);
}

static inline void
snapshot_writer_free(SnapshotWriter* self)
{
	iov_free(&self->iov);
	buf_free(&self->data);
}

static inline void
snapshot_writer_reset(SnapshotWriter* self)
{
	self->count = 0;
	iov_reset(&self->iov);
	buf_reset(&self->data);
}

static inline void
snapshot_writer_create(SnapshotWriter* self, int max)
{
	self->count_max = max;
	// MSG_SNAPSHOT_STORAGE + MSG_SNAPSHOT_ROW * max
	int size_storage_msg = sizeof(Msg) + sizeof(Uuid);
	buf_reserve(&self->data, size_storage_msg + sizeof(Msg) * max);
	iov_reserve(&self->iov, max);
}

static inline bool
snapshot_writer_has(SnapshotWriter* self)
{
	return self->count > 0;
}

static inline bool
snapshot_writer_full(SnapshotWriter* self)
{
	return self->count == self->count_max;
}

static inline void
snapshot_writer_add_storage(SnapshotWriter* self, Uuid* id)
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
snapshot_writer_add(SnapshotWriter* self, Def* def, Row* row)
{
	assert(self->count < self->count_max);

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
