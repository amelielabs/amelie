#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct RecoverSync RecoverSync;

struct RecoverSync
{
	atomic_u64 total;
	atomic_u64 complete;
	atomic_u64 errors;
};

static inline void
recover_sync_init(RecoverSync* self)
{
	memset(self, 0, sizeof(*self));
}

always_inline static inline void
recover_sync_add(RecoverSync* self)
{
	atomic_u64_inc(&self->total);
}

always_inline static inline void
recover_sync_add_complete(RecoverSync* self)
{
	atomic_u64_inc(&self->complete);
}

always_inline static inline void
recover_sync_add_error(RecoverSync* self)
{
	atomic_u64_inc(&self->errors);
}

static inline void
recover_sync(RecoverSync* self)
{
	// wait for completion
	uint64_t total = atomic_u64_of(&self->total);
	for (;;)
	{
		uint64_t complete = atomic_u64_of(&self->complete);
		if (total == complete)
			break;
		coroutine_sleep(0);
	}

	// check for errors
	uint64_t errors = atomic_u64_of(&self->errors);
	if (unlikely(errors))
		error("recover: error during playback");
}
