#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct PartMgr PartMgr;

struct PartMgr
{
	bool reference;
	List list;
	int  list_count;
};

void part_mgr_init(PartMgr*);
void part_mgr_free(PartMgr*);
void part_mgr_open(PartMgr*, bool, List*, List*);

Part*
part_mgr_find(PartMgr*, uint64_t);

Part*
part_mgr_match(PartMgr*, Uuid*);
