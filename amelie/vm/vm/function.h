#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct FunctionDef FunctionDef;
typedef struct Function    Function;
typedef struct Call        Call;
typedef struct Vm          Vm;

typedef void (*FunctionMain)(Call*);

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
	bool         context;
	List         link;
};

static inline Function*
function_allocate(FunctionDef* def)
{
	Function* self = am_malloc(sizeof(Function));
	self->main    = def->function;
	self->context = def->context;;
	str_dup_cstr(&self->schema, def->schema);
	str_dup_cstr(&self->name, def->name);
	list_init(&self->link);
	return self;
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
	// obj
	encode_obj(buf);

	// schema
	encode_raw(buf, "schema", 6);
	encode_string(buf, &self->schema);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	encode_obj_end(buf);
}
