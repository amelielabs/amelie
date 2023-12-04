#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Checkpoint Checkpoint;

struct Checkpoint
{
	bool        active;
	uint64_t    lsn;
	Rpc*        rpc;
	Lock        lock;
	LockerCache locker_cache;
	Snapshot    snapshot;
	Event       event;
	uint64_t    coroutine_id;
	Uuid*       shard;
	Db*         db;
};

void checkpoint_init(Checkpoint*, Db*, Uuid*);
void checkpoint_free(Checkpoint*);
void checkpoint_start(Checkpoint*);
void checkpoint_stop(Checkpoint*);
void checkpoint(Checkpoint*, uint64_t, Rpc*);

static inline Locker*
checkpoint_lock(Checkpoint* self, bool shared)
{
	return lock_lock(&self->lock, shared);
}

static inline void
checkpoint_unlock(Locker* locker)
{
	lock_unlock(locker);
}
