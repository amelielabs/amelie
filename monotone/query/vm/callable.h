#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Callable Callable;

struct Callable
{
	Meta*    meta;
	Code     code;
	CodeData code_data;
};

extern MetaIf callable_if;

void call(Vm*, Value*, Callable*, int, Value**);
