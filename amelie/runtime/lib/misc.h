#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

hot static inline int
compare_int64(int64_t a, int64_t b)
{
	if (a == b)
		return 0;
	return (a > b) ? 1 : -1;
}
