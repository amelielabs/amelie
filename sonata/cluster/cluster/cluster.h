#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Cluster Cluster;

struct Cluster
{
	List         list;
	int          list_count;
	FunctionMgr* function_mgr;
	Db*          db;
};

void cluster_init(Cluster*, Db*, FunctionMgr*);
void cluster_free(Cluster*);
void cluster_open(Cluster*, NodeMgr*);
void cluster_set_router(Cluster*, Router*);
void cluster_set_partition_map(Cluster*, PartMgr*);
void cluster_start(Cluster*);
void cluster_stop(Cluster*);
void cluster_recover(Cluster*);
Backend*
cluster_find(Cluster*, Uuid*);
