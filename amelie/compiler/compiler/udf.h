#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct UdfContext UdfContext;

struct UdfContext
{
	FunctionMgr* function_mgr;
	Db*          db;
};

extern UdfIf udf_if;
