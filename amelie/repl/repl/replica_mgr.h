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

typedef struct ReplicaMgr ReplicaMgr;

struct ReplicaMgr
{
	int  list_count;
	List list;
	Db*  db;
};

void replica_mgr_init(ReplicaMgr*, Db*);
void replica_mgr_free(ReplicaMgr*);
void replica_mgr_open(ReplicaMgr*);
void replica_mgr_start(ReplicaMgr*);
void replica_mgr_stop(ReplicaMgr*);
void replica_mgr_create(ReplicaMgr*, ReplicaConfig*, bool);
void replica_mgr_drop(ReplicaMgr*, Uuid*, bool);
Buf* replica_mgr_list(ReplicaMgr*, Uuid*);
Replica*
replica_mgr_find(ReplicaMgr*, Uuid*);
