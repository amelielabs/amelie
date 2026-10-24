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
	Reg            r;
	Stack          stack;
	StoreIterator* insert;
	Value*         insert_refs;
	Value*         args;
	Code*          code;
	Program*       program;
	Quota*         quota;
	Tr*            tr;
	Gtr*           gtr;
	Local*         local;
	bool           allow_close;
	Calls          calls;
};

void vm_init(Vm*, Quota*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Gtr*, Tr*, Program*, Value*,
            Return*, bool);
