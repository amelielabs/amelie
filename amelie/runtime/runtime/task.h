#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
	BufList      buf_list;
	BufCache     buf_cache;
	TaskFunction main;
	void*        main_arg;
	void*        main_arg_global;
	TaskFunction on_exit;
	void*        on_exit_arg;
	LogFunction  log_write;
	void*        log_write_arg;
	Cond         status;
	Thread       thread;
};

extern __thread Task*      am_self;
extern __thread atomic_u32 am_cancelled;

void task_init(Task*);
void task_free(Task*);
bool task_created(Task*);
int  task_create_nothrow(Task*, char*, TaskFunction, void*, void*,
                         LogFunction, void*);
void task_set_on_exit(Task*, TaskFunction, void*);
void task_detach(Task*);
void task_cancel(Task*);
void task_wait(Task*);
