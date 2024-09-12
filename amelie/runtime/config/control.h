#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Control Control;

struct Control
{
	void (*save_config)(void*);
	void (*lock)(void*, bool);
	void (*unlock)(void*);
	void*  arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
