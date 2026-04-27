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

typedef struct Branch Branch;

struct Branch
{
	Rel           rel;
	BranchConfig* config;
	Table*        table;
};

static inline void
branch_free(Branch* self, bool drop)
{
	unused(drop);
	branch_config_free(self->config);
	am_free(self);
}

static inline void
branch_show(Branch* self, Buf* buf, int flags)
{
	branch_config_write(self->config, buf, flags);
}

static inline Branch*
branch_allocate(BranchConfig* config)
{
	auto self = (Branch*)am_malloc(sizeof(Branch));
	self->config = branch_config_copy(config);
	self->table  = NULL;

	// set relation
	auto rel = &self->rel;
	rel_init(rel, REL_BRANCH);
	rel_set_user(rel, &self->config->user);
	rel_set_name(rel, &self->config->name);
	rel_set_grants(rel, &self->config->grants);
	rel_set_show(rel, (RelShow)branch_show);
	rel_set_free(rel, (RelFree)branch_free);
	rel_set_rsn(rel, state_rsn_next());
	return self;
}

static inline Branch*
branch_of(Rel* self)
{
	return (Branch*)self;
}
