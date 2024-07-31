#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Local Local;

struct Local
{
	Timezone* timezone;
};

static inline void
local_init(Local* self, Global* global)
{
	self->timezone = global->timezone;
}
