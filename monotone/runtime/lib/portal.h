#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Portal Portal;

typedef void (*PortalFunction)(Portal*, Buf*);

struct Portal
{
	PortalFunction function;
	void*          arg;
};

static inline void
portal_init(Portal* self, PortalFunction function, void *arg)
{
	self->function = function;
	self->arg      = arg;
}

static inline void
portal_write(Portal* self, Buf* msg)
{
	self->function(self, msg);
}

void portal_to_channel(Portal*, Buf*);
void portal_to_tcp(Portal*, Buf*);
void portal_to_free(Portal*, Buf*);
