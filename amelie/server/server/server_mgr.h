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

typedef struct ServerMgr ServerMgr;

struct ServerMgr
{
	List        list;
	int         list_count;
	ServerEvent on_connect;
	void*       on_connect_arg;
};

void server_mgr_init(ServerMgr*, ServerEvent, void*);
void server_mgr_free(ServerMgr*);
void server_mgr_open(ServerMgr*);
void server_mgr_start(ServerMgr*);
void server_mgr_stop(ServerMgr*);
