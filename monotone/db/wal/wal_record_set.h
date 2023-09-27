#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct WalRecordSet WalRecordSet;

struct WalRecordSet
{
	uint64_t lsn;
	int      count;
	List     list;
	int      list_count;
	Buf      data;
	Iov      iov;
};

static inline void
wal_record_set_init(WalRecordSet* self)
{
	self->lsn        = 0;
	self->count      = 0;
	self->list_count = 0;
	buf_init(&self->data);
	list_init(&self->list);
	iov_init(&self->iov);
}

static inline void
wal_record_set_free(WalRecordSet* self)
{
	buf_free(&self->data);
	iov_free(&self->iov);
}

static inline void
wal_record_set_reset(WalRecordSet* self)
{
	self->lsn        = 0;
	self->count      = 0;
	self->list_count = 0;
	list_init(&self->list);
	buf_reset(&self->data);
	iov_reset(&self->iov);
}

static inline void
wal_record_set_add(WalRecordSet* self, WalRecord* record)
{
	list_append(&self->list, &record->link);
	self->list_count++;
	self->count += record->count;
}

static inline void
wal_record_set_create(WalRecordSet* self, uint64_t lsn)
{
	// [lsn, [combined_log]]
	self->lsn = lsn;

	// [lsn, [log]]
	encode_array(&self->data, 2);
	encode_integer(&self->data, lsn);
	encode_array(&self->data, self->count);
	iov_add_buf(&self->iov, &self->data, 0, buf_size(&self->data));
	iov_prepare(&self->iov);

	// copy prepared iovec
	list_foreach(&self->list)
	{
		auto record = list_at(WalRecord, link);
		iov_import(&self->iov, &record->iov);
	}
}
