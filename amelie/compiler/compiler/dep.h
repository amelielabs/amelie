#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Dep  Dep;
typedef struct Deps Deps;

typedef enum
{
	DEP_TABLE,
	DEP_VIEW,
} DepType;

struct Dep
{
	DepType type;
	Str     schema;
	Str     name;
	List    link;
};

struct Deps
{
	List list;
	int  list_count;
};

static inline Dep*
dep_allocate(DepType type, Str* schema, Str* name)
{
	auto self = (Dep*)am_malloc(sizeof(Dep));
	self->type = type;
	str_init(&self->schema);
	str_copy(&self->schema, schema);
	str_init(&self->name);
	str_copy(&self->name, name);
	list_init(&self->link);
	return self;
}

static inline void
dep_free(Dep* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	am_free(self);
}

static inline void
deps_init(Deps* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
deps_free(Deps* self)
{
	list_foreach_safe(&self->list)
	{
		auto dep = list_at(Dep, link);
		dep_free(dep);
	}
}

static inline Dep*
deps_find(Deps* self, Str* schema, Str* name)
{
	list_foreach_safe(&self->list)
	{
		auto dep = list_at(Dep, link);
		if (str_compare(&dep->schema, schema) &&
		    str_compare(&dep->name, name))
			return dep;
	}
	return NULL;
}

static inline void
deps_add(Deps* self, DepType type, Str* schema, Str* name)
{
	auto dep = deps_find(self, schema, name);
	if (dep)
		return;
	dep = dep_allocate(type, schema, name);
	list_append(&self->list, &dep->link);
	self->list_count++;
}

static inline void
deps_import(Deps* self, TargetList* list)
{
	auto target = list->list;
	while (target)
	{
		if (target->table)
		{
			deps_add(self, DEP_TABLE, &target->table->config->schema,
			         &target->table->config->name);
		} else
		if (target->view)
		{
			deps_add(self, DEP_VIEW, &target->view->config->schema,
			         &target->view->config->name);
		}
		target = target->next;
	}
}
