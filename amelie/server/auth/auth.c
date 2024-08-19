
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>

void
auth_init(Auth* self)
{
	user_cache_init(&self->user_cache);
}

void
auth_free(Auth* self)
{
	user_cache_free(&self->user_cache);
}

void
auth_sync(Auth* self, UserCache* cache)
{
	// todo: invalidate cache
	user_cache_sync(&self->user_cache, cache);
}

bool
auth(Auth* self, Str* string)
{
	(void)self;
	(void)string;
	return false;
}
