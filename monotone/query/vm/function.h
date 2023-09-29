#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Function Function;

typedef void (*FunctionMain)(void*, Function*, Value*, int, Value**);

struct Function
{
	Str          name;
	int          argc;
	FunctionMain main;
	List         link;
};

static inline Function*
function_allocate(const char* name, int argc, FunctionMain main)
{
	Function* self = mn_malloc(sizeof(Function));
	self->argc = argc;
	self->main = main;
	guard(guard, mn_free, self);
	str_strdup(&self->name, name);
	list_init(&self->link);
	return unguard(&guard);
}

static inline void
function_free(Function* self)
{
	str_free(&self->name);
	mn_free(self);
}

static inline void
function_validate_argc(Function* self, int argc)
{
	if (unlikely(argc != self->argc))
		error("%.*s(): incorrect number of arguments", str_size(&self->name),
		      str_of(&self->name));
}

static inline void
function_validate_arg(Function* self, Value** argv, int order, int type)
{
	if (unlikely(argv[order]->type != type))
		error("%.*s(): incorrect type of %d argument", str_size(&self->name),
		      str_of(&self->name), order);
}
