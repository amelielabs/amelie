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

typedef struct LoginMgr LoginMgr;

struct LoginMgr
{
	List list;
	int  list_count;
};

void login_mgr_init(LoginMgr*);
void login_mgr_free(LoginMgr*);
void login_mgr_open(LoginMgr*, const char*);
void login_mgr_sync(LoginMgr*, const char*);
void login_mgr_add(LoginMgr*, Login*);
void login_mgr_delete(LoginMgr*, Str*);
int  login_mgr_set(LoginMgr*, Remote*, Opts*, int, char**);
Login*
login_mgr_find(LoginMgr*, Str*);
