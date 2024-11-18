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

typedef struct Cluster Cluster;

struct Cluster
{
	List         list;
	int          list_count;
	Router       router;
	FunctionMgr* function_mgr;
	Db*          db;
};

void cluster_init(Cluster*, Db*, FunctionMgr*);
void cluster_free(Cluster*);
void cluster_sync(Cluster*);
void cluster_map(Cluster*, PartMap*, Part*);

extern NodeIf cluster_if;
