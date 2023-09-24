#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct HandleIf HandleIf;
typedef struct Handle   Handle;

struct HandleIf
{
	void    (*free)(Handle*, void*);
	Handle* (*copy)(Handle*, void*);
};

struct Handle
{
	Str*      name;
	uint64_t  lsn;
	int       refs;
	HandleIf* iface;
	void*     iface_arg;
	List      link;
};

static inline void
handle_init(Handle* self)
{
	self->name      = NULL;
	self->refs      = 0;
	self->lsn       = 0;
	self->iface     = NULL;
	self->iface_arg = NULL;
	list_init(&self->link);
}

static inline void
handle_set_name(Handle* self, Str* name)
{
	self->name = name;
}

static inline void
handle_set_iface(Handle* self, HandleIf* iface, void* arg)
{
	self->iface = iface;
	self->iface_arg = arg;
}

static inline void
handle_free(Handle* self)
{
	self->iface->free(self, self->iface_arg);
}

static inline Handle*
handle_copy(Handle* self)
{
	return self->iface->copy(self, self->iface_arg);
}

static inline void
handle_ref(Handle* self)
{
	self->refs++;
}

static inline void
handle_unref(Handle* self)
{
	self->refs--;
	if (self->refs >= 0)
		return;
	handle_free(self);
}
