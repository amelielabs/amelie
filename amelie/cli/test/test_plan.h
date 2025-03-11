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

typedef struct TestPlan TestPlan;

struct TestPlan
{
	List list_env;
	List list_session;
	List list_test;
	List list_group;
};

static inline void
test_plan_init(TestPlan* self)
{
	list_init(&self->list_env);
	list_init(&self->list_session);
	list_init(&self->list_test);
	list_init(&self->list_group);
}

static inline void
test_plan_free(TestPlan* self)
{
	list_foreach_safe(&self->list_group)
	{
		auto group = list_at(TestGroup, link);
		test_group_free(group);
	}
}

static inline void
test_plan_add(TestPlan* self, Test* test)
{
	list_append(&test->group->list_test, &test->link_group);
	list_append(&self->list_test, &test->link);
}

static inline void
test_plan_add_group(TestPlan* self, TestGroup* group)
{
	list_append(&self->list_group, &group->link);
}

static inline void
test_plan_add_env(TestPlan* self, TestEnv* env)
{
	list_append(&self->list_env, &env->link);
}

static inline void
test_plan_del_env(TestPlan* self, TestEnv* env)
{
	unused(self);
	list_unlink(&env->link);
}

static inline void
test_plan_add_session(TestPlan* self, TestSession* session)
{
	list_append(&self->list_session, &session->link);
	session->env->sessions++;
}

static inline void
test_plan_del_session(TestPlan* self, TestSession* session)
{
	unused(self);
	list_unlink(&session->link);
	session->env->sessions--;
	assert(session->env->sessions >= 0);
}

static inline Test*
test_plan_find(TestPlan* self, Str* name)
{
	list_foreach(&self->list_test)
	{
		auto test = list_at(Test, link);
		if (str_compare(&test->name, name))
			return test;
	}
	return NULL;
}

static inline TestGroup*
test_plan_find_group(TestPlan* self, Str* name)
{
	list_foreach(&self->list_group)
	{
		auto group = list_at(TestGroup, link);
		if (str_compare(&group->name, name))
			return group;
	}
	return NULL;
}

static inline TestEnv*
test_plan_find_env(TestPlan* self, Str* name)
{
	list_foreach(&self->list_env)
	{
		auto env = list_at(TestEnv, link);
		if (str_compare(&env->name, name))
			return env;
	}
	return NULL;
}

static inline TestSession*
test_plan_find_session(TestPlan* self, Str* name)
{
	list_foreach(&self->list_session)
	{
		auto session = list_at(TestSession, link);
		if (str_compare(&session->name, name))
			return session;
	}
	return NULL;
}

void test_plan_read(TestPlan*, Str*);
