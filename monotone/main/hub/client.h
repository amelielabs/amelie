#pragma once

//
// monotone
//
// SQL OLTP database
//

void hub_client(Hub*, Client*);
void hub_native(Hub*, Native*);

static inline void*
hub_session_create(Hub* self, Portal* portal)
{
	return self->iface->session_create(&self->share, portal);
}

static inline void
hub_session_free(Hub* self, void* session)
{
	self->iface->session_free(session);
}

static inline void
hub_execute(Hub* self, void* session, Buf* buf)
{
	self->iface->session_execute(session, buf);
}
