#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Task Task;

struct Task
{
	CoroutineMgr   coroutine_mgr;
	TimerMgr       timer_mgr;
	Poller         poller;
	ConditionCache condition_cache;
	Channel        channel;
	BufCache       buf_cache;
	MainFunction   main;
	void*          main_arg;
	void*          main_arg_global;
	Coroutine*     main_coroutine;
	char*          name;
	LogFunction    log;
	void*          log_arg;
	ThreadStatus   thread_status;
	Thread         thread;
};

extern __thread Task* in_task;

static inline Coroutine*
in_self(void)
{
	return in_task->coroutine_mgr.current;
}

void task_init(Task*);
void task_free(Task*);
bool task_active(Task*);
int  task_create_nothrow(Task*, char*, MainFunction, void*, void*,
                         LogFunction, void*);
void task_wait(Task*);
void task_coroutine_main(void*);
