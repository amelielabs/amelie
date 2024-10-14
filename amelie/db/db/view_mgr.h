#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct ViewMgr ViewMgr;

struct ViewMgr
{
	HandleMgr mgr;
};

void  view_mgr_init(ViewMgr*);
void  view_mgr_free(ViewMgr*);
void  view_mgr_create(ViewMgr*, Tr*, ViewConfig*, bool);
void  view_mgr_drop(ViewMgr*, Tr*, Str*, Str*, bool);
void  view_mgr_rename(ViewMgr*, Tr*, Str*, Str*, Str*, Str*, bool);
void  view_mgr_dump(ViewMgr*, Buf*);
View* view_mgr_find(ViewMgr*, Str*, Str*, bool);
Buf*  view_mgr_list(ViewMgr*);
