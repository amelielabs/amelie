#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct WalWrite WalWrite;
typedef struct WalBatch WalBatch;

struct WalWrite
{
	uint32_t crc;
	uint64_t lsn;
	uint32_t size;
	uint32_t count;
	uint32_t offset;
} packed;

struct WalBatch
{
	WalWrite header;
	Iov      iov;
	int      list_count;
	List     list;
};

static inline void
wal_batch_init(WalBatch* self)
{
	memset(&self->header, 0, sizeof(self->header));
	self->list_count = 0;
	list_init(&self->list);
	iov_init(&self->iov);
}

static inline void
wal_batch_free(WalBatch* self)
{
	iov_free(&self->iov);
}

static inline void
wal_batch_reset(WalBatch* self)
{
	memset(&self->header, 0, sizeof(self->header));
	self->list_count = 0;
	list_init(&self->list);
	iov_reset(&self->iov);
}

static inline void
wal_batch_begin(WalBatch* self, uint64_t lsn)
{
	auto header = &self->header;
	header->lsn  = lsn;
	header->size = sizeof(self->header);
	iov_add(&self->iov, header, sizeof(self->header));
}

static inline void
wal_batch_add(WalBatch* self, LogSet* log_set)
{
	// [header][rows meta][rows]
	iov_add_buf(&self->iov, &log_set->meta);

	auto header = &self->header;
	header->offset += buf_size(&log_set->meta);
	header->size   += buf_size(&log_set->meta);
	header->size   += log_set->iov.size;
	header->count  += log_set->iov.iov_count;

	list_append(&self->list, &log_set->link);
	self->list_count++;
}
