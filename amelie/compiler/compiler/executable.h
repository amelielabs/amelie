#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Executable Executable;

struct Executable
{
	Code     code;
	Code     code_node;
	CodeData code_data;
	Program  program;
};

static inline Executable*
executable_allocate(void)
{
	Executable* self = am_malloc(sizeof(*self));
	code_init(&self->code);
	code_init(&self->code_node);
	code_data_init(&self->code_data);
	program_init(&self->program);
	return self;
}

static inline void
executable_free(Executable* self)
{
	code_free(&self->code);
	code_free(&self->code_node);
	code_data_free(&self->code_data);
	am_free(self);
}

static inline void
executable_set(Executable* self, Program* program)
{
	code_copy(&self->code, program->code);
	code_copy(&self->code_node, program->code_node);
	code_data_copy(&self->code_data, program->code_data);

	self->program = *program;
	self->program.code = &self->code;
	self->program.code_node = &self->code_node;
	self->program.code_data = &self->code_data;
}
