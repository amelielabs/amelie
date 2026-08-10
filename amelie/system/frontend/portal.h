#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct Portal Portal;

struct Portal
{
	User*    user;
	Lock*    lock;
	Endpoint endpoint;
	Local    local;
	Output   output;
};

static inline void
portal_lock(Portal* self, LockId id)
{
	// take catalog lock
	if (self->lock)
	{
		if (self->lock->rel_lock == id)
			return;
		unlock(self->lock);
		self->lock = NULL;
	}
	self->lock = lock_system(REL_CATALOG, id);
	if (self->lock)
		lock_detach(self->lock);
}

static inline void
portal_unlock(Portal* self)
{
	if (self->lock)
	{
		unlock(self->lock);
		self->lock = NULL;
	}
}

hot static inline void
portal_set_local(Portal* self, bool with_limits)
{
	// set limits
	auto local = &self->local;
	if (with_limits)
		local->limits = &self->user->config->limits;

	// set timezone
	local->timezone = self->output.timezone;
	str_set_str(&local->user, &self->user->config->name);

	// set time
	local->time_us = opt_int_of(&self->endpoint.time);
	local->time_ms = local->time_us / 1000;

	// set random seed
	auto random = &local->random;
	random->seed[0] = opt_int_of(&self->endpoint.seed);
	random->seed[1] = random->seed[0] ^ local->time_us;
}

hot static inline void
portal_auth(Portal* self, Auth* auth_ref)
{
	// take catalog lock
	self->lock = lock_system(REL_CATALOG, LOCK_SHARED);
	lock_detach(self->lock);

	// authenticate user
	auto endpoint = &self->endpoint;
	auto trusted  = opt_int_of(&endpoint->trusted);
	auto token    = opt_string_of(&endpoint->token);
	auto user_id  = opt_string_of(&endpoint->user);
	self->user = auth(auth_ref, user_id, token, !trusted);

	// superuser can connect only from localhost/unixsocket (trusted source)
	if (self->user->config->superuser && !trusted)
		error("auth: superuser can connect only from localhost");

	// check permissions
	switch (opt_int_of(&endpoint->endpoint)) {
	case ENDPOINT_SQL:
		user_check(self->user, PERM_SQL);
		break;
	case ENDPOINT_API:
		user_check(self->user, PERM_API);
		break;
	case ENDPOINT_MCP:
		break;
	case ENDPOINT_BACKUP:
	case ENDPOINT_REPL:
		user_check(self->user, PERM_SERVICE);
		break;
	}

	// configure local
	portal_set_local(self, true);

	// set output local (enforce send limit)
	output_set_local(&self->output, &self->local);
}

hot static inline void
portal_auth_as(Portal* self, Str* user, bool with_limits)
{
	// take catalog lock
	self->lock = lock_system(REL_CATALOG, LOCK_SHARED);
	lock_detach(self->lock);

	// find user
	self->user = catalog_find_user(&share()->db->catalog, user, true);

	// configure local
	portal_set_local(self, with_limits);

	// set output local (enforce send limit)
	output_set_local(&self->output, &self->local);
}

static inline void
portal_reset(Portal* self, bool with_endpoint)
{
	portal_unlock(self);
	self->user = NULL;
	if (with_endpoint)
		endpoint_reset(&self->endpoint);
	output_reset(&self->output);
	local_reset(&self->local);
}

static inline void
portal_free(Portal* self)
{
	portal_reset(self, true);
	endpoint_free(&self->endpoint);
	output_free(&self->output);
}

static inline void
portal_init(Portal* self)
{
	self->user = NULL;
	self->lock = NULL;
	endpoint_init(&self->endpoint);
	local_init(&self->local);
	output_init(&self->output);
}

static inline void
portal_prepare(Portal* self)
{
	// timestamp
	auto endpoint = &self->endpoint;
	opt_int_set(&endpoint->time, time_us());

	// random seed
	int64_t seed = random_generate(&am_task->random);
	opt_int_set(&endpoint->seed, seed);

	// set timezone
	auto timezone = &endpoint->timezone.string;
	if (str_empty(timezone))
		str_set_str(timezone, &runtime()->timezone->name);
}
