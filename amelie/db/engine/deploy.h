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

typedef struct DeployIf DeployIf;
typedef struct Deploy   Deploy;

struct DeployIf
{
	void (*attach_all)(Deploy*, Level*);
	void (*attach)(Deploy*, Part*);
	void (*detach_all)(Deploy*, Level*);
	void (*detach)(Deploy*, Part*);
};

struct Deploy
{
	DeployIf* iface;
	void*     iface_arg;
};

static inline void
deploy_init(Deploy* self, DeployIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
}

static inline void
deploy_attach_all(Deploy* self, Level* level)
{
	self->iface->attach_all(self, level);
}

static inline void
deploy_attach(Deploy* self, Part* part)
{
	self->iface->attach(self, part);
}

static inline void
deploy_detach_all(Deploy* self, Level* level)
{
	self->iface->detach_all(self, level);
}

static inline void
deploy_detach(Deploy* self, Part* part)
{
	self->iface->detach(self, part);
}
