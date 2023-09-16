#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Control Control;

struct Control
{
	Channel*  core;
	Channel*  server;
	void    (*save_config)(void*);
	void    (*checkpoint)(void*, bool);
	void*     arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
