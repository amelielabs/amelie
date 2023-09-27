#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Transaction Transaction;

struct Transaction
{
	uint64_t lsn;
	bool     active;
	bool     aborted;
	bool     auto_commit;
	Log      log;
	void*    arg;
};

static inline void
transaction_init(Transaction* self)
{
	self->lsn         = 0;
	self->active      = false;
	self->aborted     = false;
	self->auto_commit = false;
	self->arg         = NULL;
	log_init(&self->log);
}

static inline void
transaction_free(Transaction* self)
{
	log_free(&self->log);
}

static inline bool
transaction_active(Transaction* self)
{
	return self->active;
}

static inline bool
transaction_read_only(Transaction* self)
{
	return self->log.count == 0;
}

static inline void
transaction_set_auto_commit(Transaction* self)
{
	self->auto_commit = true;
}

static inline bool
transaction_is_single_statement(Transaction* self)
{
	return self->auto_commit;
}

static inline bool
transaction_is_multi_statement(Transaction* self)
{
	return !self->auto_commit;
}

static inline uint64_t
transaction_lsn(Transaction* self)
{
	return self->lsn;
}

static inline void
transaction_set_lsn(Transaction* self, uint64_t lsn)
{
	self->lsn = lsn;
}
