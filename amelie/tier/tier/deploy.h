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
	void (*attach)(Deploy*, VolumeMgr*);
	void (*detach)(Deploy*, VolumeMgr*);
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
deploy_attach(Deploy* self, VolumeMgr* mgr)
{
	self->iface->attach(self, mgr);
}

static inline void
deploy_detach(Deploy* self, VolumeMgr* mgr)
{
	self->iface->detach(self, mgr);
}
