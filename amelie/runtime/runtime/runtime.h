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

typedef struct RuntimeIf RuntimeIf;
typedef struct Runtime   Runtime;

typedef void (*RuntimeMain)(char*, int, char**);

typedef enum
{
	RUNTIME_ERROR,
	RUNTIME_OK,
	RUNTIME_COMPLETE
} RuntimeStatus;

struct RuntimeIf
{
	Channel*  system;
	void    (*save_state)(void*);
	void*     arg;
};

struct Runtime
{
	BufMgr      buf_mgr;
	Config      config;
	State       state;
	RuntimeIf*  iface;
	Timezone*   timezone;
	TimezoneMgr timezone_mgr;
	CrcFunction crc;
	Random      random;
	Resolver    resolver;
	Logger      logger;
	Task        task;
};

void          runtime_init(Runtime*);
void          runtime_free(Runtime*);
RuntimeStatus runtime_start(Runtime*, RuntimeMain, char*, int, char**);
void          runtime_stop(Runtime*);

static inline bool
runtime_started(Runtime* self)
{
	return task_active(&self->task);
}
