
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

void
config_local_init(ConfigLocal* self)
{
	memset(self, 0, sizeof(*self));
	list_init(&self->list);
}

void
config_local_free(ConfigLocal* self)
{
	list_foreach_safe(&self->list) {
		auto var = list_at(Var, link);
		var_free(var);
	}
}

void
config_local_prepare(ConfigLocal* self)
{
	// timezone
	auto var = &self->timezone;
	var_init(var, "timezone", VAR_STRING, VAR_L);
	list_append(&self->list, &var->link);
}

Var*
config_local_find(ConfigLocal* self, Str* name)
{
	list_foreach(&self->list) {
		auto var = list_at(Var, link);
		if (str_compare(&var->name, name))
			return var;
	}
	return NULL;
}
