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

struct Task
{
	BufMgr*      buf_mgr;
	Poller       poller;
	CoroutineMgr coroutine_mgr;
	TimerMgr     timer_mgr;
	Bus          bus;
	Channel      channel;
	MainFunction main;
	void*        main_arg;
	void*        main_arg_global;
	void*        main_arg_share;
	Coroutine*   main_coroutine;
	LogFunction  log_write;
	void*        log_write_arg;
	char         name[9];
	Cond         status;
	Thread       thread;
};

extern __thread void* am_share;
extern __thread void* am_global;
extern __thread Task* am_task;

static inline Coroutine*
am_self(void)
{
	return am_task->coroutine_mgr.current;
}

void task_init(Task*);
void task_free(Task*);
bool task_active(Task*);
int  task_create_nothrow(Task*, char*, MainFunction, void*, void*, void*,
                         LogFunction, void*,
                         BufMgr*);
void task_wait(Task*);
void task_coroutine_main(void*);
