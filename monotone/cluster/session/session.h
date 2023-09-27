#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Ctx Ctx;
typedef struct Session Session;

struct Session
{
	Transaction trx;
	Locker*     cat_locker;
	RequestSet  req_set;
	Portal*     portal;
	Share*      share;
};

void session_init(Session*, Share*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
