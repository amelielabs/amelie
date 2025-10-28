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

typedef struct Scan Scan;

typedef void (*ScanFunction)(Scan*);

struct Scan
{
	Ast*         expr_where;
	int          roffset;
	int          rlimit;
	ScanFunction on_match;
	void*        on_match_arg;
	Buf          breaks;
	Buf          continues;
	From*        from;
	Compiler*    compiler;
};

void scan(Compiler*, From*, Ast*, Ast*, Ast*,
          ScanFunction, void*);
