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

typedef struct Vm Vm;

struct Vm
{
	Reg       r;
	Stack     stack;
	Code*     code;
	CodeData* code_data;
	Buf*      code_arg;
	uint8_t*  upsert;
	bool      allow_close;
	Value*    refs;
	Value*    args;
	Part*     part;
	Gtr*      gtr;
	Program*  program;
	Tr*       tr;
	Local*    local;
	Fns       fns;
};

void vm_init(Vm*, Part*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Gtr*, Tr*, Program*, Code*, CodeData*, Buf*,
            Value*, Value*, Return*, bool, int);
