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
	Buf*         code_arg_buf;
	Buf*         args;
	CursorMgr    cursor_mgr;
	Uuid*        node;
	Executor*    executor;
	Dtr*         dtr;
	Result*      cte;
	Value*       result;
	Buf*         body;
	Tr*          tr;
	Local*       local;
	CallMgr      call_mgr;
	FunctionMgr* function_mgr;
	Db*          db;
};

void vm_init(Vm*, Db*, Uuid*, Executor*, Dtr*, Buf*, FunctionMgr*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Local*, Tr*, Code*, CodeData*, Buf*, Buf*, Buf*,
            Result*, Value*, int);
