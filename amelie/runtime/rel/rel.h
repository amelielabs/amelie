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

typedef struct Rel Rel;

typedef void (*RelFree)(Rel*, bool);
typedef void (*RelShow)(Rel*, Buf*, int);

typedef enum
{
	REL_UNDEF,
	REL_USER,
	REL_STORAGE,
	REL_TABLE,
	REL_BRANCH,
	REL_UDF,
	REL_TOPIC,
	REL_SUBSCRIPTION,
	REL_LOCK,
	REL_SYSTEM
} RelType;

struct Rel
{
	Str*     user;
	Str*     name;
	RelType  type;
	Uuid*    id;
	Grants*  grants;
	RelShow  show;
	RelFree  free;
	Hashnode link_ht;
	Hashnode link_htid;
	List     link;

	// lock manager
	Spinlock lock;
	uint64_t lock_order;
	int      lock_set[LOCK_MAX];
	List     lock_wait;
	int      lock_wait_count;
};

static inline const char*
rel_type_of(RelType type)
{
	switch (type) {
	case REL_UNDEF:        return "relation";
	case REL_USER:         return "user";
	case REL_STORAGE:      return "storage";
	case REL_TABLE:        return "table";
	case REL_BRANCH:       return "branch";
	case REL_UDF:          return "function";
	case REL_TOPIC:        return "topic";
	case REL_SUBSCRIPTION: return "subscription";
	case REL_LOCK:         return "lock";
	case REL_SYSTEM:       return "system";
	default:
		break;
	}
	abort();
	return NULL;;
}

static inline void
rel_init(Rel* self, RelType type)
{
	self->user            = NULL;
	self->name            = NULL;
	self->id              = NULL;
	self->type            = type;
	self->grants          = NULL;
	self->show            = NULL;
	self->free            = NULL;
	self->lock_order      = 0;
	self->lock_wait_count = 0;
	spinlock_init(&self->lock);
	hashnode_init(&self->link_ht);
	hashnode_init(&self->link_htid);
	list_init(&self->link);
	memset(self->lock_set, 0, sizeof(self->lock_set));
	list_init(&self->lock_wait);
}

static inline void
rel_set_user(Rel* self, Str* user)
{
	self->user = user;
}

static inline void
rel_set_name(Rel* self, Str* name)
{
	self->name = name;
}

static inline void
rel_set_id(Rel* self, Uuid* id)
{
	self->id = id;
}

static inline void
rel_set_grants(Rel* self, Grants* grants)
{
	self->grants = grants;
}

static inline void
rel_set_show(Rel* self, RelShow func)
{
	self->show = func;
}

static inline void
rel_set_free(Rel* self, RelFree func)
{
	self->free = func;
}

static inline void
rel_set_rsn(Rel* self, uint64_t value)
{
	self->lock_order = value;
}

static inline void
rel_show(Rel* self, Buf* buf, int flags)
{
	if (self->show)
		self->show(self, buf, flags);
}

static inline void
rel_free(Rel* self)
{
	spinlock_free(&self->lock);
	if (self->free)
		self->free(self, false);
}

static inline void
rel_drop(Rel* self)
{
	spinlock_free(&self->lock);
	if (self->free)
		self->free(self, true);
}

static inline void
rel_permission_error(Rel* self)
{
	error("relation '{str}.{str}': permission denied",
	      self->user, self->name);
}
