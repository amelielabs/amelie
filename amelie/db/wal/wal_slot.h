#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct WalSlot WalSlot;

struct WalSlot
{
	atomic_u64 lsn;
	bool       attached;
	Mutex      lock;
	CondVar    cond_var;
	List       link;
};

static inline void
wal_slot_init(WalSlot* self)
{
	self->lsn      = 0;
	self->attached = false;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	list_init(&self->link);
}

static inline void
wal_slot_free(WalSlot* self)
{
	assert(! self->attached);
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
}

static inline void
wal_slot_set(WalSlot* self, uint64_t lsn)
{
	atomic_u64_set(&self->lsn, lsn);
}

static inline void
wal_slot_wait(WalSlot* self)
{
	mutex_lock(&self->lock);
	cond_var_wait(&self->cond_var, &self->lock);
	mutex_unlock(&self->lock);
}

static inline void
wal_slot_signal(WalSlot* self, uint64_t lsn)
{
	if (lsn <= atomic_u64_of(&self->lsn))
		return;
	mutex_lock(&self->lock);
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}
