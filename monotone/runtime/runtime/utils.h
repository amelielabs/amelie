#pragma once

//
// monotone
//
// SQL OLTP database
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
