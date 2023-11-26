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
	void    (*save_state)(void*);
	void    (*save_catalog)(void*);
	void*     arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
