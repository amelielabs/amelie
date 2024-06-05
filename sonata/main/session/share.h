#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Share Share;

struct Share
{
	Executor*    executor;
	Cluster*     cluster;
	FrontendMgr* frontend_mgr;
	FunctionMgr* function_mgr;
	UserMgr*     user_mgr;
	CatalogMgr*  catalog_mgr;
	Db*          db;
};
