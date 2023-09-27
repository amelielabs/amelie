#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Core Core;

struct Core
{
	Share        share;
	// db
	Db           db;
	// cluster
	HubMgr       hub_mgr;
	ShardMap     shard_map;
	ShardMgr     shard_mgr;
	RequestSched req_sched;
	// server
	Server       server;
	UserMgr      user_mgr;
	// config state
	CatalogMgr   catalog_mgr;
	ConfigState  config_state;
	Control      control;
};

Core*
core_create(void);
void core_free(Core*);
void core_start(Core*, bool);
void core_stop(Core*);
void core_main(Core*);
