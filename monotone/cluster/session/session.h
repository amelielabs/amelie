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
	RequestSet  req_set;
	LogSet      log_set;
	Locker*     cat_locker;
	Portal*     portal;
	Share*      share;
};

void session_init(Session*, Share*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
