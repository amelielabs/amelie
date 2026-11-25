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
	Request  req;
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
	request_reset(&self->req);
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
	request_init(&self->req);
	endpoint_init(&self->endpoint);
	local_init(&self->local);
	output_init(&self->output);
}

static inline void
portal_prepare(Portal* self)
{
	// timestamp
	auto endpoint = &self->endpoint;
	endpoint->time = time_us();

	// random seed
	endpoint->seed = random_generate(&am_task->random);

	// set timezone
	auto timezone = &endpoint->timezone;
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

	// set db id
	endpoint->db_id = *self->db->rel.id;
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
	local->time_us = self->endpoint.time;
	local->time_ms = local->time_us / 1000;

	// set random seed
	auto random = &local->random;
	random->seed[0] = self->endpoint.seed;
	random->seed[1] = random->seed[0] ^ local->time_us;
}

hot static inline void
portal_set(Portal* self, Db* db, bool with_limits)
{
	// set db
	self->db = db;
	str_set_str(&self->endpoint.db, &self->db->config->name);

	// set db id
	self->endpoint.db_id = self->db->config->id;

	// configure local
	portal_set_local(self, with_limits);

	// set output local (enforce send limit)
	output_set_local(&self->output, &self->local);
}

hot static inline void
portal_bind(Portal* self, Uuid* id)
{
	// take storage lock
	portal_lock(self, LOCK_SHARED);

	auto rel = catalog_find_by(&share()->storage->catalog, REL_DB, id, true);
	portal_set(self, db_of(rel), false);
}

hot static inline void
portal_bind_name(Portal* self, Str* name)
{
	// take storage lock
	portal_lock(self, LOCK_SHARED);

	auto db = catalog_find_db(&share()->storage->catalog, name, true);
	portal_set(self, db, false);
}
