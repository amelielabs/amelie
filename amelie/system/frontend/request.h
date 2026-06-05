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

typedef struct Request Request;

struct Request
{
	User*    user;
	Lock*    lock;
	Endpoint endpoint;
	Output   output;
};

static inline void
request_lock(Request* self, LockId id)
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
request_unlock(Request* self)
{
	if (self->lock)
	{
		unlock(self->lock);
		self->lock = NULL;
	}
}

hot static inline void
request_auth(Request* self, Auth* auth_ref)
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
}

static inline void
request_reset(Request* self, bool with_endpoint)
{
	request_unlock(self);
	self->user = NULL;
	if (with_endpoint)
		endpoint_reset(&self->endpoint);
	output_reset(&self->output);
}

static inline void
request_free(Request* self)
{
	request_reset(self, true);
	endpoint_free(&self->endpoint);
	output_free(&self->output);
}

static inline void
request_init(Request* self)
{
	self->user = NULL;
	self->lock = NULL;
	endpoint_init(&self->endpoint);
	output_init(&self->output);
}
