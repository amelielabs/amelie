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

typedef struct FunctionMgr FunctionMgr;

struct FunctionMgr
{
	Hashtable ht;
	List      list;
	int       list_count;
};

void function_mgr_init(FunctionMgr*);
void function_mgr_free(FunctionMgr*);
void function_mgr_add(FunctionMgr*, Function*);
void function_mgr_del(FunctionMgr*, Function*);
Function*
function_mgr_find(FunctionMgr*, Str*, Str*);
