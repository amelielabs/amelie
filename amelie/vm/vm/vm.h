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
	Reg          r;
	Stack        stack;
	Code*        code;
	CodeData*    code_data;
	Buf*         code_arg;
	CursorMgr    cursor_mgr;
	Executor*    executor;
	Part*        part;
	Dtr*         dtr;
	Tr*          tr;
	Result*      cte;
	Value*       result;
	Content*     content;
	Local*       local;
	CallMgr      call_mgr;
	FunctionMgr* function_mgr;
	Db*          db;
};

void vm_init(Vm*, Db*, Executor*, FunctionMgr*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Part*, Dtr*, Tr*, Code*, CodeData*, Buf*,
            Result*, Value*, Content*, int);
