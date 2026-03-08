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

typedef struct PartArg PartArg;

struct PartArg
{
	atomic_u64*   heap_total;
	Sequence*     seq;
	bool          unlogged;
	Uuid*         id_table;
	Partitioning* config;
};
