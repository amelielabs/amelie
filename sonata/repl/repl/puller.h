#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Puller Puller;

struct Puller
{
	Client*     client;
	Plan        plan;
	Lock        lock;
	Locker*     locker;
	Locker*     locker_exclusive;
	LockerCache locker_cache;
	Task        task;
	Executor*   executor;
	Db*         db;
};

void puller_init(Puller*, Db*, Executor*);
void puller_free(Puller*);
void puller_start(Puller*);
void puller_stop(Puller*);
void puller_lock(Puller*);
void puller_unlock(Puller*);
