#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Vm Vm;

struct Vm
{
	Reg          r;
	Stack        stack;
	Code*        code;
	CodeData*    code_data;
	int          argc;
	Value**      argv;
	CursorMgr    cursor_mgr;
	FunctionMgr* function_mgr;
	Uuid*        shard;
	Command*     command;
	Dispatch*    dispatch;
	Portal*      portal;
	Transaction* trx;
	Db*          db;
};

void vm_init(Vm*, Db*, FunctionMgr*, Uuid*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Transaction*, Dispatch*, Command*, Code*, CodeData*,
            int, Value**, Portal*);
