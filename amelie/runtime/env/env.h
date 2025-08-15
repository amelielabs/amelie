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

typedef struct EnvControl EnvControl;
typedef struct Env        Env;

struct EnvControl
{
	Channel*  system;
	void    (*save_state)(void*);
	void*     arg;
};

struct Env
{
	BufMgr      buf_mgr;
	Config      config;
	State       state;
	Timezone*   timezone;
	TimezoneMgr timezone_mgr;
	CrcFunction crc;
	EnvControl* control;
	Random      random;
	Resolver    resolver;
	Logger      logger;
};

void env_init(Env*);
void env_free(Env*);
void env_start(Env*);
void env_stop(Env*);
bool env_create(Env*, char*);
bool env_open(Env*, char*, int, char**);
