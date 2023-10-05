#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Ctx Ctx;
typedef struct Session Session;

typedef enum
{
	LOCK_NONE,
	LOCK_SHARED,
	LOCK_EXCLUSIVE
} SessionLock;

struct Session
{
	Compiler    compiler;
	Command     cmd;
	Transaction trx;
	// catalog lock
	SessionLock lock;
	RequestLock lock_req;
	Locker*     lock_shared;
	// request
	RequestSet  req_set;
	LogSet      log_set;
	Portal*     portal;
	Share*      share;
};

void session_init(Session*, Share*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
