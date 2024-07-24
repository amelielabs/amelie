#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

enum
{
	MATCH_EQU,
	MATCH_NEQU,
	MATCH_LTE,
	MATCH_LT,
	MATCH_GTE,
	MATCH_GT
};

void value_any(Value*, Value*, Value*, int);
