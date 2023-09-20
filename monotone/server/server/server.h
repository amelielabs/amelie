#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Server Server;

struct Server
{
	List   listen;
	int    listen_count;
	List   config;
	int    config_count;
	HubMgr hub_mgr;
};

void server_init(Server*);
void server_free(Server*);
void server_start(Server*, HubIf*, void*, bool);
void server_stop(Server*);
