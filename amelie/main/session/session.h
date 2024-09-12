#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Session Session;

enum
{
	LOCK_NONE,
	LOCK_SHARED,
	LOCK_EXCLUSIVE
};

struct Session
{
	Vm        vm;
	Compiler  compiler;
	Dtr       tr;
	Explain   explain;
	Client*   client;
	Local     local;
	int       lock;
	Share*    share;
	Task      task;
	List      link;
};

Session*
session_create(Client*, Share*);
void session_free(Session*);
void session_lock(Session*, int);
void session_unlock(Session*);
void session_start(Session*);
void session_stop(Session*);
