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

hot static inline int
compare_int64(int64_t a, int64_t b)
{
	if (a == b)
		return 0;
	return (a > b) ? 1 : -1;
}
