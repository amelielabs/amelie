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
	Db*      db;
	Lock*    lock;
	Endpoint endpoint;
	Local    local;
	Output   output;
};

static inline void
portal_lock(Portal* self, LockId id)
{
	// take storage lock
	if (self->lock)
	{
		if (self->lock->lock == id)
			return;
		unlock(self->lock);
		self->lock = NULL;
	}
	self->lock = lock_storage(id);
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

static inline void
portal_reset(Portal* self, bool with_endpoint)
{
	portal_unlock(self);
	self->db = NULL;
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
	self->db   = NULL;
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
	str_set_str(timezone, &runtime()->timezone->name);
}

hot static inline void
portal_auth(Portal* self, Auth* auth, Token* token)
{
	// take storage lock
	portal_lock(self, LOCK_SHARED);

	// authenticate token
	auto endpoint = &self->endpoint;
	self->db = auth_run(auth, endpoint, token);
}

hot static inline void
portal_set_local(Portal* self, bool with_limits)
{
	// set limits
	auto local = &self->local;
	if (with_limits)
		local->limits = &self->db->config->limits;

	// set timezone
	local->timezone = self->output.timezone;
	str_set_str(&local->db, &self->db->config->name);

	// set time
	local->time_us = opt_int_of(&self->endpoint.time);
	local->time_ms = local->time_us / 1000;

	// set random seed
	auto random = &local->random;
	random->seed[0] = opt_int_of(&self->endpoint.seed);
	random->seed[1] = random->seed[0] ^ local->time_us;
}

hot static inline void
portal_set(Portal* self, Str* db, bool with_limits)
{
	// take storage lock
	portal_lock(self, LOCK_SHARED);

	// find db
	self->db = catalog_find_db(&share()->storage->databases, db, true);

	// configure local
	portal_set_local(self, with_limits);

	// set output local (enforce send limit)
	output_set_local(&self->output, &self->local);
}
