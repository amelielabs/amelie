#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Node Node;

struct Node
{
	Route       route;
	TrxList     prepared;
	Vm          vm;
	NodeConfig* config;
	Task        task;
	List        link;
};

Node*
node_allocate(NodeConfig*, Db*, FunctionMgr*);
void node_free(Node*);
void node_start(Node*);
void node_stop(Node*);
