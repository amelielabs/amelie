#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Session Session;

typedef enum
{
	SESSION_LOCK_NONE,
	SESSION_LOCK,
	SESSION_LOCK_EXCLUSIVE
} SessionLock;

struct Session
{
	Vm          vm;
	Compiler    compiler;
	Plan        plan;
	Explain     explain;
	Client*     client;
	Local       local;
	SessionLock lock_type;
	Locker*     lock;
	Frontend*   frontend;
	Share*      share;
};

Session*
session_create(Client*, Frontend*, Share*);
void session_free(Session*);
void session_lock(Session*, SessionLock);
void session_unlock(Session*);
void session_main(Session*);
