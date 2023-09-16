#pragma once

//
// monotone
//
// SQL OLTP database
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
	self = mn_malloc(sizeof(*self));
	str_init(&self->name);
	str_init(&self->secret);
	return self;
}

static inline void
user_config_free(UserConfig* self)
{
	str_free(&self->name);
	str_free(&self->secret);
	mn_free(self);
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

static inline void
user_config_set_digest(UserConfig* self, Str* password)
{
	// create digest out of password
	uint8_t digest[SHA1_DIGEST_LENGTH];
	Sha1 sha1;
	sha1_init(&sha1);
	sha1_add_str(&sha1, password);
	sha1_end(&sha1, digest);

	// convert digest to string
	char digest_string[SHA1_DIGEST_STRING_LENGTH];
	sha1_to_string(digest, digest_string, sizeof(digest_string));

	// set secret
	str_strndup(&self->secret, digest_string, sizeof(digest_string) - 1);
}

static inline UserConfig*
user_config_copy(UserConfig* self)
{
	auto copy = user_config_allocate();
	guard(copy_guard, user_config_free, copy);
	user_config_set_name(copy, &self->name);
	user_config_set_secret(copy, &self->secret);
	return unguard(&copy_guard);
}

static inline UserConfig*
user_config_read(uint8_t** pos)
{
	auto self = user_config_allocate();
	guard(self_guard, user_config_free, self);
	// map
	int count;
	data_read_map(pos, &count);
	// name
	data_skip(pos);
	data_read_string_copy(pos, &self->name);
	// secret
	data_skip(pos);
	data_read_string_copy(pos, &self->secret);
	return unguard(&self_guard);
}

static inline void
user_config_write(UserConfig* self, Buf* buf)
{
	encode_map(buf, 2);
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);
	encode_raw(buf, "secret", 6);
	encode_string(buf, &self->secret);
}

static inline void
user_config_write_safe(UserConfig* self, Buf* buf)
{
	encode_map(buf, 2);
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);
	encode_raw(buf, "password", 8);
	encode_bool(buf, !str_empty(&self->secret));
}
