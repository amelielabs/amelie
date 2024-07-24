#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Control Control;

struct Control
{
	Channel*  system;
	void    (*save_config)(void*);
	void*     arg;
};

static inline void
control_init(Control* self)
{
	memset(self, 0, sizeof(*self));
}
