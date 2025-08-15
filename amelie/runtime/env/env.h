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

typedef struct Instance Instance;

struct Instance
{
	Logger      logger;
	Random      random;
	Resolver    resolver;
	Config      config;
	State       state;
	TimezoneMgr timezone_mgr;
	BufMgr      buf_mgr;
	Global      global;
};

void instance_init(Instance*);
void instance_free(Instance*);
void instance_start(Instance*);
void instance_stop(Instance*);
bool instance_create(Instance*, char*);
bool instance_open(Instance*, char*, int, char**);
