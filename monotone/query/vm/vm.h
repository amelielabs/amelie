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
	int          argc;
	Value**      argv;
	CursorMgr    cursor_mgr;
	FunctionMgr* function_mgr;
	Uuid*        shard;
	Command*     command;
	Portal*      portal;
	Transaction* trx;
	Db*          db;
};

void vm_init(Vm*, Db*, FunctionMgr*, Transaction*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Code*, int, Value**, Uuid*, Command*, Portal*);
