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

typedef struct BackendMgr BackendMgr;

struct BackendMgr
{
	List         list;
	int          list_count;
	Router       router;
	FunctionMgr* function_mgr;
	Db*          db;
};

void backend_mgr_init(BackendMgr*, Db*, FunctionMgr*);
void backend_mgr_free(BackendMgr*);
void backend_mgr_sync(BackendMgr*);
void backend_mgr_map(BackendMgr*, PartMap*, Part*);

extern WorkerIf backend_mgr_if;
