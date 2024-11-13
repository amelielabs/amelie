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

typedef struct Call Call;
typedef struct Vm   Vm;

typedef enum
{
	CALL_EXECUTE,
	CALL_CLEANUP
} CallType;

struct Call
{
	int       argc;
	Value*    argv;
	Value*    result;
	Vm*       vm;
	CallType  type;
	Function* function;
	void**    context;
};

static inline void
call_validate(Call* self, int argc)
{
	if (unlikely(argc != self->argc))
		error("%.*s(): expected %d arguments, but got %d", str_size(&self->function->name),
		      str_of(&self->function->name), argc, self->argc);
}

static inline void
call_validate_arg(Call* self, int order, ValueType type)
{
	if (unlikely(self->argv[order].type != type))
		error("%.*s(): expected %s for argument %d, but got %s", str_size(&self->function->name),
		      str_of(&self->function->name),
		      value_type_to_string(type),
		      order,
		      value_type_to_string(self->argv[order].type));
}
