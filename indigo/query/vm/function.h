#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Function Function;

typedef void (*FunctionMain)(void*, Function*, Value*, int, Value**);

struct Function
{
	Str          schema;
	Str          name;
	int          argc;
	FunctionMain main;
	List         link;
};

static inline Function*
function_allocate(const char*  schema,
                  const char*  name,
                  int          argc,
                  FunctionMain main)
{
	Function* self = in_malloc(sizeof(Function));
	self->argc = argc;
	self->main = main;
	guard(guard, in_free, self);
	str_strdup(&self->schema, schema);
	str_strdup(&self->name, name);
	list_init(&self->link);
	return unguard(&guard);
}

static inline void
function_free(Function* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	in_free(self);
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

static inline void
function_write(Function* self, Buf* buf)
{
	// map
	encode_map(buf, 3);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// argc
	encode_raw(buf, "argc", 4);
	encode_integer(buf, self->argc);
}
