#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
	char*          name;
	LogFunction    log_write;
	void*          log_write_arg;
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
