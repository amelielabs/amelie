#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Repl Repl;

typedef enum
{
	REPL_PRIMARY,
	REPL_REPLICA
} ReplRole;

struct Repl
{
	ReplRole   role;
	ReplicaMgr replica_mgr;
};

void repl_init(Repl*, Db*);
void repl_free(Repl*);
void repl_open(Repl*);
void repl_start(Repl*);
void repl_stop(Repl*);
void repl_subscribe(Repl*, Str*);
Buf* repl_show(Repl*);
