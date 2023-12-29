#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct LogSet LogSet;

struct LogSet
{
	uint64_t lsn;
	int      count;
	List     list;
	int      list_count;
	Buf      data;
	Iov      iov;
};

static inline void
log_set_init(LogSet* self)
{
	self->lsn        = 0;
	self->count      = 0;
	self->list_count = 0;
	buf_init(&self->data);
	list_init(&self->list);
	iov_init(&self->iov);
}

static inline void
log_set_free(LogSet* self)
{
	buf_free(&self->data);
	iov_free(&self->iov);
}

static inline void
log_set_reset(LogSet* self)
{
	self->lsn        = 0;
	self->count      = 0;
	self->list_count = 0;
	list_init(&self->list);
	buf_reset(&self->data);
	iov_reset(&self->iov);
}

static inline void
log_set_add(LogSet* self, Log* log)
{
	list_append(&self->list, &log->link);
	self->list_count++;
	self->count += log->count_persistent;
}

static inline void
log_set_create(LogSet* self, uint64_t lsn)
{
	self->lsn = lsn;

#if 0
	// WAL_WRITE
	buf_reserve(&self->data, sizeof(Msg));
	auto msg = (Msg*)(self->data.start);
	msg->id   = MSG_WAL_WRITE;
	msg->size = 0;
	buf_advance(&self->data, sizeof(Msg));

	// [lsn, [log]]
	encode_array(&self->data, 2);
	encode_integer(&self->data, lsn);
	encode_array(&self->data, self->count);
	iov_add_buf(&self->iov, &self->data, 0, buf_size(&self->data));
	iov_prepare(&self->iov);

	// copy prepared iovec
	list_foreach(&self->list)
	{
		auto log = list_at(Log, link);
		iov_import(&self->iov, &log->data_iov);
	}

	// set message size
	msg = (Msg*)(self->data.start);
	msg->size = self->iov.size;
#endif
}
