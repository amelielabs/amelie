#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct FunctionDef FunctionDef;
typedef struct Function    Function;
typedef struct Call        Call;
typedef struct Vm          Vm;

typedef void (*FunctionMain)(Call*);

struct Call
{
	int       argc;
	Value**   argv;
	Value*    result;
	Vm*       vm;
	Function* function;
};

struct FunctionDef
{
	const char*  schema;
	const char*  name;
	FunctionMain function;
	bool         context;
};

struct Function
{
	Str          schema;
	Str          name;
	FunctionMain main;
	List         link;
};

static inline Function*
function_allocate(FunctionDef* def)
{
	Function* self = am_malloc(sizeof(Function));
	self->main = def->function;
	guard(am_free, self);
	str_strdup(&self->schema, def->schema);
	str_strdup(&self->name, def->name);
	list_init(&self->link);
	return unguard();
}

static inline void
function_free(Function* self)
{
	str_free(&self->schema);
	str_free(&self->name);
	am_free(self);
}

static inline void
function_write(Function* self, Buf* buf)
{
	// map
	encode_map(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_map_end(buf);
}

static inline void
call_validate(Call* self, int argc)
{
	if (unlikely(argc != self->argc))
		error("%.*s(): incorrect number of arguments", str_size(&self->function->name),
		      str_of(&self->function->name));
}

static inline void
call_validate_arg(Call* self, int order, ValueType type)
{
	if (unlikely(self->argv[order]->type != type))
		error("%.*s(): incorrect type of %d argument", str_size(&self->function->name),
		      str_of(&self->function->name), order);
}
