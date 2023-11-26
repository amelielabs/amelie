#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct PartMgr PartMgr;

struct PartMgr
{
	List list;
	int  list_count;
};

void  part_mgr_init(PartMgr*);
void  part_mgr_free(PartMgr*);
void  part_mgr_open(PartMgr*);
void  part_mgr_gc(PartMgr*);

Part* part_mgr_create(PartMgr*, PartConfig*);
void  part_mgr_attach(PartMgr*, Part*, Index*);
void  part_mgr_detach(PartMgr*, Part*, Index*);
Buf*  part_mgr_list(PartMgr*);

Part* part_mgr_find(PartMgr*, Uuid*);
Part* part_mgr_find_for(PartMgr*, Uuid*, Uuid*);
Part* part_mgr_find_for_table(PartMgr*, Uuid*);
