#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Session Session;

struct Session
{
	Vm        vm;
	Compiler  compiler;
	Plan      plan;
	Explain   explain;
	Client*   client;
	Local     local;
	int       lock_type;
	Lock*     lock;
	Lock*     lock_ref;
	Frontend* frontend;
	Share*    share;
};

Session*
session_create(Client*, Frontend*, Share*);
void session_free(Session*);
void session_lock(Session*, int);
void session_unlock(Session*);
void session_main(Session*);
