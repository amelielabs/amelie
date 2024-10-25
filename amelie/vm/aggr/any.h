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
	MATCH_EQU,
	MATCH_NEQU,
	MATCH_LTE,
	MATCH_LT,
	MATCH_GTE,
	MATCH_GT
};

void value_any(Value*, Value*, Value*, int);
