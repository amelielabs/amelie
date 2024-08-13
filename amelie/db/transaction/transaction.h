#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Transaction Transaction;

struct Transaction
{
	bool   active;
	bool   aborted;
	Log    log;
	Limit* limit;
	void*  arg;
};

static inline void
transaction_init(Transaction* self)
{
	self->active  = false;
	self->aborted = false;
	self->limit   = NULL;
	self->arg     = NULL;
	log_init(&self->log);
}

static inline void
transaction_reset(Transaction* self)
{
	self->active  = false;
	self->aborted = false;
	self->limit   = NULL;
	self->arg     = NULL;
	log_reset(&self->log);
}

static inline void
transaction_free(Transaction* self)
{
	log_free(&self->log);
}

static inline void
transaction_set_limit(Transaction* self, Limit* limit)
{
	self->limit = limit;
}

static inline bool
transaction_active(Transaction* self)
{
	return self->active;
}

static inline bool
transaction_aborted(Transaction* self)
{
	return self->aborted;
}

static inline bool
transaction_read_only(Transaction* self)
{
	return self->log.count == 0;
}
