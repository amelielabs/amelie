#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct UserMgr UserMgr;

struct UserMgr
{
	RelMgr mgr;
};

void  user_mgr_init(UserMgr*);
void  user_mgr_free(UserMgr*);
bool  user_mgr_create(UserMgr*, Tr*, UserConfig*, bool);
bool  user_mgr_drop(UserMgr*, Tr*, Str*, bool);
bool  user_mgr_rename(UserMgr*, Tr*, Str*, Str*, bool);
bool  user_mgr_revoke(UserMgr*, Tr*, Str*, Str*, bool);
bool  user_mgr_grant(UserMgr*, Tr*, Str*, bool, uint32_t, bool);
void  user_mgr_dump(UserMgr*, Buf*);
void  user_mgr_list(UserMgr*, Buf*, Str*, bool, int);
User* user_mgr_find(UserMgr*, Str*, bool);
