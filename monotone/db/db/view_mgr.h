#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ViewMgr ViewMgr;

struct ViewMgr
{
	HandleMgr mgr;
};

void  view_mgr_init(ViewMgr*);
void  view_mgr_free(ViewMgr*);
void  view_mgr_create(ViewMgr*, Transaction*, ViewConfig*, bool);
void  view_mgr_drop(ViewMgr*, Transaction*, Str*, Str*, bool);
void  view_mgr_rename(ViewMgr*, Transaction*, Str*, Str*, Str*, Str*, bool);
void  view_mgr_dump(ViewMgr*, Buf*);
View* view_mgr_find(ViewMgr*, Str*, Str*, bool);
Buf*  view_mgr_list(ViewMgr*);
