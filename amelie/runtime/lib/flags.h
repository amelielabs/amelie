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
	// encode minimal output
	FMINIMAL = 1 << 0,

	// include metrics
	FMETRICS = 1 << 1,

	// include sensitive information
	FSECRETS = 1 << 2
};

static inline bool
flags_has(int flags, int mask)
{
	return (flags & mask) > 0;
}
