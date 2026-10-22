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
	uint8_t*  upsert;
	bool      allow_close;
	Value*    args;
	Value*    refs;
	Gtr*      gtr;
	Program*  program;
	Quota*    quota;
	Tr*       tr;
	Local*    local;
	Calls     calls;
};

void vm_init(Vm*, Quota*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Gtr*, Tr*, Program*, Value*,
            Return*, bool);
