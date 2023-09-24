#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaMgr MetaMgr;

struct MetaMgr
{
	MetaCache cache;
	MetaIf*   iface;
	void*     iface_arg;
};

void meta_mgr_init(MetaMgr*, MetaIf*, void*);
void meta_mgr_free(MetaMgr*);
void meta_mgr_create(MetaMgr*, Transaction*, MetaConfig*, bool);
void meta_mgr_drop(MetaMgr*, Transaction*, Str*, bool);
