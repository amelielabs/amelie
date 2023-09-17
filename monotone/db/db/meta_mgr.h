#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct MetaMgr MetaMgr;

struct MetaMgr
{
	HandleMgr  metas;
	MetaIface* iface;
	void*      iface_arg;
};

void  meta_mgr_init(MetaMgr*, MetaIface*, void*);
void  meta_mgr_free(MetaMgr*);
void  meta_mgr_gc(MetaMgr*, uint64_t);
void  meta_mgr_create(MetaMgr*, Transaction*, MetaConfig*, bool);
void  meta_mgr_drop(MetaMgr*, Transaction*, Str*, bool);
Meta* meta_mgr_find(MetaMgr*, Str*, bool);
Buf*  meta_mgr_show(MetaMgr*, MetaType);
