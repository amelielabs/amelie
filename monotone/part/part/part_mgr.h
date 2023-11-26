#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct PartMgr PartMgr;

struct PartMgr
{
	HandleMgr mgr;
};

void  part_mgr_init(PartMgr*);
void  part_mgr_free(PartMgr*);
void  part_mgr_open(PartMgr*, CatalogMgr*);
Part* part_mgr_create(PartMgr*, Transaction*, PartConfig*);
void  part_mgr_drop_by_id(PartMgr*, Transaction*, Uuid*);
void  part_mgr_drop_by_id_table(PartMgr*, Transaction*, Uuid*);

Part* part_mgr_find(PartMgr*, Uuid*);
Part* part_mgr_find_for(PartMgr*, Uuid*, Uuid*);
Part* part_mgr_find_for_table(PartMgr*, Uuid*);
Buf*  part_mgr_list(PartMgr*);
void  part_mgr_dump(PartMgr*, Buf*);
