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

typedef struct PlayerSync PlayerSync;

struct PlayerSync
{
	atomic_u64 total;
	atomic_u64 complete;
	atomic_u64 errors;
	atomic_u32 shutdown;
};

static inline void
player_sync_init(PlayerSync* self)
{
	memset(self, 0, sizeof(*self));
}

always_inline static inline void
player_sync_add(PlayerSync* self)
{
	atomic_u64_inc(&self->total);
}

always_inline static inline void
player_sync_add_complete(PlayerSync* self)
{
	atomic_u64_inc(&self->complete);
}

always_inline static inline void
player_sync_add_error(PlayerSync* self)
{
	atomic_u64_inc(&self->errors);
}

always_inline static inline void
player_sync_add_shutdown(PlayerSync* self)
{
	atomic_u32_inc(&self->shutdown);
}

static inline void
player_sync_shutdown(PlayerSync* self, int count)
{
	// wait for completion
	for (;;)
	{
		int done = atomic_u32_of(&self->shutdown);
		if (done == count)
			break;
		coroutine_sleep(0);
	}
}

static inline void
player_sync(PlayerSync* self)
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
		error("player: error during playback");
}
