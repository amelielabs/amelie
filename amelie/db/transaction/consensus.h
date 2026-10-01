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

typedef struct Consensus       Consensus;
typedef struct ConsensusAtomic ConsensusAtomic;

struct Consensus
{
	uint64_t commit;
	uint64_t abort;
};

struct ConsensusAtomic
{
	_Alignas(16) _Atomic __int128 state;
};

static inline void
consensus_init(Consensus* self)
{
	self->commit = 0;
	self->abort  = 0;
}

static inline void
consensus_atomic_init(ConsensusAtomic* self)
{
	atomic_init(&self->state, 0);
}

static inline void
consensus_atomic_read(ConsensusAtomic* self, Consensus* consensus)
{
	__int128 state = atomic_load_explicit(&self->state, memory_order_acquire);
	memcpy(consensus, &state, sizeof(Consensus));
}

static inline void
consensus_atomic_write(ConsensusAtomic* self, Consensus* consensus)
{
	__int128 state;
	memcpy(&state, consensus, sizeof(Consensus));
	atomic_store_explicit(&self->state, state, memory_order_release);
}
