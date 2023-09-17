#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Core Core;

struct Core
{
	// database
	Db          db;
	// server
	ClientMgr   client_mgr;
	Server      server;
	// config state
	CatalogMgr  catalog_mgr;
	ConfigState config_state;
	Control     control;
};

Core*
core_create(void);
void core_free(Core*);
void core_start(Core*, bool);
void core_stop(Core*);
void core_main(Core*);
