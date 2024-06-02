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
	Router       router;
	FunctionMgr* function_mgr;
	Db*          db;
};

void  cluster_init(Cluster*, Db*, FunctionMgr*);
void  cluster_free(Cluster*);
void  cluster_open(Cluster*, bool);
void  cluster_set_partition_map(Cluster*, PartMgr*);
void  cluster_start(Cluster*);
void  cluster_stop(Cluster*);
void  cluster_recover(Cluster*);
void  cluster_create(Cluster*, NodeConfig*, bool);
void  cluster_drop(Cluster*, Uuid*, bool);
void  cluster_alter(Cluster*, NodeConfig*, bool);
Node* cluster_find(Cluster*, Uuid*);
