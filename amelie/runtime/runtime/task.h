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
	CoroutineMgr   coroutine_mgr;
	TimerMgr       timer_mgr;
	Poller         poller;
	Bus            bus;
	Channel        channel;
	BufMgr*        buf_mgr;
	MainFunction   main;
	void*          main_arg;
	void*          main_arg_global;
	Coroutine*     main_coroutine;
	LogFunction    log_write;
	void*          log_write_arg;
	char           name[9];
	Cond           status;
	Thread         thread;
};

extern __thread Task* am_task;

static inline Coroutine*
am_self(void)
{
	return am_task->coroutine_mgr.current;
}

void task_init(Task*);
void task_free(Task*);
bool task_active(Task*);
int  task_create_nothrow(Task*, char*, MainFunction, void*, void*,
                         LogFunction, void*,
                         BufMgr*);
void task_wait(Task*);
void task_coroutine_main(void*);
