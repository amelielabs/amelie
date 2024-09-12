#pragma once

//
// amelie.
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
void  cluster_start(Cluster*);
void  cluster_stop(Cluster*);
void  cluster_sync(Cluster*);
void  cluster_create(Cluster*, NodeConfig*, bool);
void  cluster_drop(Cluster*, Uuid*, bool);
void  cluster_map(Cluster*, PartMap*, Part*);
Buf*  cluster_list(Cluster*);
Node* cluster_find(Cluster*, Uuid*);
