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

typedef struct Consensus Consensus;

struct Consensus
{
	uint64_t commit;
	uint64_t abort;
};

static inline void
consensus_init(Consensus* self)
{
	self->commit = 0;
	self->abort  = 0;
}
