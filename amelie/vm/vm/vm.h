#pragma once

//
// amelie.
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
void vm_run(Vm*, Local*, Tr*, Code*, CodeData*, Buf*,
            Result*, Value*, int);
