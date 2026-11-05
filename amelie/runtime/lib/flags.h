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

enum
{
	// encode SQL CREATE schema
	FCREATE  = 1 << 0,

	// encode minimal output
	FMINIMAL = 1 << 1,

	// FROM SHOW
	FFROM    = 1 << 2
};

static inline bool
flags_has(int flags, int mask)
{
	return (flags & mask) > 0;
}
