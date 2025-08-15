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

typedef struct Env Env;

struct Env
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

void env_init(Env*);
void env_free(Env*);
void env_start(Env*);
void env_stop(Env*);
bool env_create(Env*, char*);
bool env_open(Env*, char*, int, char**);
