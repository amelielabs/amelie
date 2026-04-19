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
	Str      content;
	Endpoint endpoint;
	Output   output;
};

static inline void
request_init(Request* self)
{
	self->user = NULL;
	self->lock = NULL;
	str_init(&self->content);
	endpoint_init(&self->endpoint);
	output_init(&self->output);
}

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

static inline void
request_reset(Request* self)
{
	self->user = NULL;
	request_unlock(self);
	endpoint_reset(&self->endpoint);
	output_reset(&self->output);
	str_init(&self->content);
}

static inline void
request_free(Request* self)
{
	request_reset(self);
	endpoint_free(&self->endpoint);
}

static inline void
request_auth(Request* self, Auth* auth_ref)
{
	// take catalog lock
	self->lock = lock_system(REL_CATALOG, LOCK_SHARED);
	lock_detach(self->lock);

	// authenticate user
	auto endpoint = &self->endpoint;
	auto token = &endpoint->token.string;
	auto token_required = opt_int_of(&endpoint->auth);
	auto user_id = &endpoint->user.string;
	self->user = auth(auth_ref, user_id, token, token_required);
}
