#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Repl Repl;

struct Repl
{
	ReplRole  role;
	PusherMgr pusher_mgr;
	NodeMgr*  node_mgr;
	Db*       db;
};

void repl_init(Repl*, Db*, NodeMgr*);
void repl_free(Repl*);
void repl_open(Repl*);
void repl_start(Repl*);
void repl_stop(Repl*);
void repl_promote(Repl*, Str*, Str*);
