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
	REL_DATABASE,
	REL_TABLE,
	REL_UDF
} RelType;

struct Rel
{
	Str*     name;
	RelType  type;
	Str*     description;
	RelShow  show;
	RelFree  free;
	Hashnode link_ht;
	List     link;
};

static inline const char*
rel_type_of(RelType type)
{
	switch (type) {
	case REL_UNDEF:    return "relation";
	case REL_DATABASE: return "database";
	case REL_TABLE:    return "table";
	case REL_UDF:      return "function";
	default:
		break;
	}
	abort();
	return NULL;;
}

static inline void
rel_init(Rel* self, RelType type)
{
	self->name        = NULL;
	self->type        = type;
	self->description = NULL;
	self->show        = NULL;
	self->free        = NULL;
	hashnode_init(&self->link_ht);
	list_init(&self->link);
}

static inline void
rel_set_name(Rel* self, Str* name)
{
	self->name = name;
}

static inline void
rel_set_description(Rel* self, Str* description)
{
	self->description = description;
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
rel_show(Rel* self, Buf* buf, int flags)
{
	if (self->show)
		self->show(self, buf, flags);
}

static inline void
rel_free(Rel* self)
{
	if (self->free)
		self->free(self, false);
}

static inline void
rel_drop(Rel* self)
{
	if (self->free)
		self->free(self, true);
}

static inline void
rel_permission_error(Rel* self)
{
	error("relation '{str}': permission denied",
	      self->name);
}

static inline void
rel_write(Rel* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_str(buf, self->name);

	// type
	encode_raw(buf, "type", 4);
	encode_cstr(buf, rel_type_of(self->type));

	// description
	encode_raw(buf, "description", 11);
	if (self->description)
		encode_str(buf, self->description);
	else
		encode_raw(buf, "", 0);

	encode_obj_end(buf);
}
