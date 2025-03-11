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

typedef struct Task Task;

typedef void (*TaskFunction)(void*);

struct Task
{
	ExceptionMgr exception_mgr;
	Error        error;
	Channel      channel;
	Clock        clock;
	Poller       poller;
	Arena        arena;
	BufMgr*      buf_mgr;
	TaskFunction main;
	void*        main_arg;
	void*        main_arg_global;
	LogFunction  log_write;
	void*        log_write_arg;
	bool         log_cancel;
	char         name[9];
	Cond         status;
	Thread       thread;
};

extern __thread Task*      am_self;
extern __thread atomic_u32 am_cancelled;

void task_init(Task*);
void task_free(Task*);
bool task_created(Task*);
int  task_create_nothrow(Task*, char*, TaskFunction, void*, void*,
                         LogFunction, void*,
                         BufMgr*);
void task_cancel(Task*);
void task_wait(Task*);
