#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Control Control;

enum
{
	CONTROL_SHOW_USERS,
	CONTROL_SHOW_REPL,
	CONTROL_SHOW_REPLICAS,
	CONTROL_SHOW_NODES
};

struct Control
{
	void (*save_config)(void*);
	void (*lock)(void*, bool);
	void (*unlock)(void*);
	Buf* (*show)(void*, int);
	void*  arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
