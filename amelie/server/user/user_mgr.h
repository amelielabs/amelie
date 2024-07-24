#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct UserMgr UserMgr;

struct UserMgr
{
	UserCache cache;
};

void user_mgr_init(UserMgr*);
void user_mgr_free(UserMgr*);
void user_mgr_open(UserMgr*);
void user_mgr_create(UserMgr*, UserConfig*, bool);
void user_mgr_drop(UserMgr*, Str*, bool);
void user_mgr_alter(UserMgr*, UserConfig*);
Buf* user_mgr_list(UserMgr*);
