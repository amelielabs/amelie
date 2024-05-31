#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct NodeMgr NodeMgr;

struct NodeMgr
{
	int  list_count;
	List list;
};

void  node_mgr_init(NodeMgr*);
void  node_mgr_free(NodeMgr*);
void  node_mgr_open(NodeMgr*);
void  node_mgr_create(NodeMgr*, NodeConfig*, bool);
void  node_mgr_drop(NodeMgr*, Str*, bool);
Buf*  node_mgr_list(NodeMgr*);
Node* node_mgr_find(NodeMgr*, Str*);
Node* node_mgr_find_by_id(NodeMgr*, Uuid*);
