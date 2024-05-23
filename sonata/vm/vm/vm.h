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
	Uuid*        shard;
	Executor*    executor;
	Plan*        plan;
	Result*      cte;
	Value*       result;
	Body*        body;
	Transaction* trx;
	FunctionMgr* function_mgr;
	Db*          db;
};

void vm_init(Vm*, Db*, Uuid*, Executor*, Plan*, Body*, FunctionMgr*);
void vm_free(Vm*);
void vm_reset(Vm*);
void vm_run(Vm*, Transaction*, Code*, CodeData*, Buf*,
            Result*, Value*, int);
