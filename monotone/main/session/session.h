#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Session Session;

struct Session
{
	Transaction trx;
	Portal*     portal;
	Db*         db;
};

Session*
session_create(Db*, ClientMgr*, Portal*);
void session_free(Session*);
bool session_execute(Session*, Buf*);
