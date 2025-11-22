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

typedef struct Test      Test;
typedef struct TestGroup TestGroup;

struct Test
{
	Str        name;
	TestGroup* group;
	List       link_group;
	List       link;
};

static inline Test*
test_create(Str* name, TestGroup* group)
{
	Test* self = am_malloc(sizeof(*self));
	str_copy(&self->name, name);
	self->group = group;
	list_init(&self->link_group);
	list_init(&self->link);
	return self;
}

static inline void
test_free(Test* self)
{
	str_free(&self->name);
	am_free(self);
}
