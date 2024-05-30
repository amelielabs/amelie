#pragma once

//
// sonata.
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
	Locker*   lock;
	Frontend* frontend;
	Share*    share;
};

Session*
session_create(Client*, Frontend*, Share*);
void session_free(Session*);
void session_main(Session*);
