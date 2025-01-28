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

typedef struct ComputeMgr ComputeMgr;

struct ComputeMgr
{
	List         list;
	int          list_count;
	Router       router;
	FunctionMgr* function_mgr;
	Db*          db;
};

void compute_mgr_init(ComputeMgr*, Db*, FunctionMgr*);
void compute_mgr_free(ComputeMgr*);
void compute_mgr_sync(ComputeMgr*);
void compute_mgr_map(ComputeMgr*, PartMap*, Part*);

extern NodeIf compute_mgr_if;
