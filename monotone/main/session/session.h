#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Session Session;

struct Session
{
	Vm          vm;
	Compiler    compiler;
	Command     cmd;
	// session lock
	Locker*     lock;
	// request
	Explain     explain;
	Dispatch    dispatch;
	LogSet      log_set;
	Portal*     portal;
	Share*      share;
};

Session*
session_create(Share*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
