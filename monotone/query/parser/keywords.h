#pragma once

//
// monotone
//
// SQL OLTP database
//

#include "grammar.h"

typedef struct Keyword Keyword;

struct Keyword
{
	int         id;
	const char* name;
	int         name_size;
};

extern Keyword* keywords[26];
