#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct NodeMgr NodeMgr;

struct NodeMgr
{
	NodeIf*   iface;
	void*     iface_arg;
	HandleMgr mgr;
};

void  node_mgr_init(NodeMgr*, NodeIf*, void*);
void  node_mgr_free(NodeMgr*);
void  node_mgr_create(NodeMgr*, Tr*, NodeConfig*, bool);
void  node_mgr_drop(NodeMgr*, Tr*, Str*, bool);
void  node_mgr_dump(NodeMgr*, Buf*);
Buf*  node_mgr_list(NodeMgr*);
Node* node_mgr_find(NodeMgr*, Str*, bool);
