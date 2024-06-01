#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	Uuid*        node;
	Executor*    executor;
	Plan*        plan;
	Result*      cte;
	Value*       result;
	Buf*         body;
	Transaction* trx;
	FunctionMgr* function_mgr;
	Db*          db;
};

void vm_init(Vm*, Db*, Uuid*, Executor*, Plan*, Buf*, FunctionMgr*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Transaction*, Code*, CodeData*, Buf*,
            Result*, Value*, int);
