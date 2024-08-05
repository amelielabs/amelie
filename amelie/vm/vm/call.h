#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
	Value**   argv;
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
