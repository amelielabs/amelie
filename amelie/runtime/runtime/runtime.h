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

typedef void (*RuntimeMain)(void*, int, char**);

typedef enum
{
	RUNTIME_ERROR,
	RUNTIME_OK,
	RUNTIME_COMPLETE
} RuntimeStatus;

struct RuntimeIf
{
	void (*save_state)(void*);
	void*  arg;
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
	CodecCache  cache_compression;
	JobMgr      job_mgr;
	Logger      logger;
	LockSystem  lock_system;
	Task        task;
};

void          runtime_init(Runtime*);
void          runtime_free(Runtime*);
RuntimeStatus runtime_start(Runtime*, RuntimeMain, void*, int, char**);
void          runtime_stop(Runtime*);

static inline bool
runtime_started(Runtime* self)
{
	return task_active(&self->task);
}
