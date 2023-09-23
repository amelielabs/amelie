#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HandleMgr HandleMgr;

struct HandleMgr
{
	List             list;
	int              list_count;
	HandleDestructor free_function;
	void*            free_arg;
};

void handle_mgr_init(HandleMgr*);
void handle_mgr_free(HandleMgr*);
void handle_mgr_set_free(HandleMgr*, HandleDestructor, void*);
void handle_mgr_gc(HandleMgr*, uint64_t);
int  handle_mgr_count(HandleMgr*);
void handle_mgr_set(HandleMgr*, Handle*);
void handle_mgr_unset(HandleMgr*, Handle*);
Handle*
handle_mgr_get(HandleMgr*, Str*);

static inline void
handle_free(Handle* self, HandleMgr* mgr)
{
	mgr->free_function(self, mgr->free_arg);
}
