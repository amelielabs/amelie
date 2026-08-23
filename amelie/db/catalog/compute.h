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

typedef struct Compute Compute;

struct Compute
{
	Rel            rel;
	ComputeConfig* config;
};

bool compute_create(Catalog*, Tr*, ComputeConfig*, bool);

always_inline static inline Compute*
compute_of(Rel* self)
{
	return (Compute*)self;
}
