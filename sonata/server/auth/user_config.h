#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct UserConfig UserConfig;

struct UserConfig
{
	Str name;
	Str secret;
};

static inline UserConfig*
user_config_allocate(void)
{
	UserConfig* self;
	self = so_malloc(sizeof(*self));
	str_init(&self->name);
	str_init(&self->secret);
	return self;
}

static inline void
user_config_free(UserConfig* self)
{
	str_free(&self->name);
	str_free(&self->secret);
	so_free(self);
}

static inline void
user_config_set_name(UserConfig* self, Str* name)
{
	str_copy(&self->name, name);
}

static inline void
user_config_set_secret(UserConfig* self, Str* secret)
{
	str_copy(&self->secret, secret);
}

static inline UserConfig*
user_config_copy(UserConfig* self)
{
	auto copy = user_config_allocate();
	guard(user_config_free, copy);
	user_config_set_name(copy, &self->name);
	user_config_set_secret(copy, &self->secret);
	return unguard();
}

static inline void
user_config_sync(UserConfig* self, UserConfig* with)
{
	if (! str_compare(&self->secret, &with->secret))
	{
		str_free(&self->secret);
		user_config_set_secret(self, &with->secret);
	}
}

static inline UserConfig*
user_config_read(uint8_t** pos)
{
	auto self = user_config_allocate();
	guard(user_config_free, self);
	Decode map[] =
	{
		{ DECODE_STRING, "name",   &self->name   },
		{ DECODE_STRING, "secret", &self->secret },
		{ 0,              NULL,    NULL          },
	};
	decode_map(map, pos);
	return unguard();
}

static inline void
user_config_write(UserConfig* self, Buf* buf, bool safe)
{
	if (safe)
	{
		encode_map(buf, 2);
		encode_raw(buf, "name", 4);
		encode_string(buf, &self->name);
		encode_raw(buf, "secret", 6);
		encode_string(buf, &self->secret);
		return;
	}
	encode_map(buf, 1);
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);
}
