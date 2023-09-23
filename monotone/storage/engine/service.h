#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Service Service;

struct Service
{
	bool        active;
	List        list;
	int         list_count;
	uint64_t    worker_id;
	Event       event;
	CompactReq  compact_req;
	CompactMgr* compact_mgr;
	void*       engine;
};

void service_init(Service*, CompactMgr*, void*);
void service_free(Service*);
void service_start(Service*);
void service_stop(Service*);
void service_add(Service*, Part*);
void service_remove(Service*, Part*);
Part*
service_next(Service*);
