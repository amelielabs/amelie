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
	Reg*      regs;
	Stack     stack;
	Code*     code;
	CodeData* code_data;
	Buf*      code_arg;
	int       upsert;
	Value*    args;
	Core*     core;
	Dtr*      dtr;
	Program*  program;
	Value*    result;
	Content*  content;
	Tr*       tr;
	Local*    local;
	CallMgr   call_mgr;
};

void vm_init(Vm*, Core*, Dtr*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Tr*, Program*, Code*, CodeData*, Buf*, Reg*, Value*,
            Value*, Content*, int);
