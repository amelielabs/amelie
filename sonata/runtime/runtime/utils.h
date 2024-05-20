#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

static inline char*
strnchr(char* string, int size, int c)
{
	char* end = string + size;
	for (; string < end; string++)
		if (*string == c)
			return string;
	return NULL;
}

hot static inline int
compare_int64(int64_t a, int64_t b)
{
	if (a == b)
		return 0;
	return (a > b) ? 1 : -1;
}
