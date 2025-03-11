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

typedef struct TestGroup TestGroup;

struct TestGroup
{
	Str  name;
	Str  directory;
	List list_test;
	List link;
};

static inline TestGroup*
test_group_create(Str* name, Str* directory)
{
	TestGroup* self = am_malloc(sizeof(*self));
	str_copy(&self->name, name);
	str_copy(&self->directory, directory);
	list_init(&self->list_test);
	list_init(&self->link);
	return self;
}

static inline void
test_group_free(TestGroup* self)
{
	list_foreach_safe(&self->list_test)
	{
		auto test = list_at(Test, link_group);
		test_free(test);
	}
	str_free(&self->name);
	str_free(&self->directory);
	am_free(self);
}
