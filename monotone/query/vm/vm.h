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
	CursorMgr    cursor_mgr;
	FunctionMgr* function_mgr;
	Uuid*        shard;
	Command*     command;
	Dispatch*    dispatch;
	Result*      result;
	Portal*      portal;
	Transaction* trx;
	Db*          db;
};

void vm_init(Vm*, Db*, FunctionMgr*, Uuid*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Transaction*, Dispatch*, Command*,
            Code*, CodeData*, Result*,
            Portal*);
