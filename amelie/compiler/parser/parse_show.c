
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_parser.h>

void
parse_show(Stmt* self)
{
	// SHOW <section> [name] [ON name] [ALL]
	auto stmt = ast_show_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// section | option name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME && name->id != KSTRING)
		stmt_error(self, name, "name expected");
	stmt->section = name->string;

	// [name]
	name = stmt_next(self);
	if (name->id == KNAME || name->id == KSTRING || name->id == KPRIMARY)
		stmt->name = name->string;
	else
		stmt_push(self, name);

	// [ON name]
	if (stmt_if(self, KON))
	{
		name = stmt_next_shadow(self);
		if (name->id != KNAME && name->id != KSTRING)
			stmt_error(self, name, "name expected");
		stmt->on = name->string;
	}

	// [ALL]
	stmt->all = stmt_if(self, KALL) != NULL;

	// set returning column
	auto column = column_allocate();
	column_set_name(column, &stmt->section);
	column_set_type(column, TYPE_JSON, -1);
	columns_add(&stmt->ret.columns, column);
}

Ast*
parse_show_func(Stmt* self)
{
	// section | option name
	auto section = stmt_next_shadow(self);
	if (section->id != KNAME && section->id != KSTRING)
		stmt_error(self, section, "name expected");
	section->id = KSTRING;

	// [name]
	auto name = stmt_next(self);
	if (! (name->id == KNAME || name->id == KSTRING || name->id == KPRIMARY))
	{
		stmt_push(self, name);
		name = ast(KSTRING);
	}
	name->id = KSTRING;

	// [ON name]
	Ast* on;
	if (stmt_if(self, KON))
	{
		on = stmt_next_shadow(self);
		if (on->id != KNAME && on->id != KSTRING)
			stmt_error(self, on, "name expected");
	} else {
		on = ast(KSTRING);
	}
	on->id = KSTRING;

	// [ALL]
	auto all = ast(KFALSE);
	if (stmt_if(self, KALL))
		all->id = KTRUE;

	// show(section, name, on, all)
	auto args_list = section;
	section->next = name;
	name->next    = on;
	on->next      = all;
	all->next     = NULL;

	// args(list_head, NULL)
	auto args = ast_args_allocate();
	args->ast.l       = args_list;
	args->ast.r       = NULL;
	args->ast.integer = 4;
	args->constable   = false;

	// func(NULL, args)
	Str fn;
	str_set(&fn, "show_from", 9);

	auto func = ast_func_allocate();
	func->fn    = function_mgr_find(share()->function_mgr, &fn);
	func->ast.l = NULL;
	func->ast.r = &args->ast;
	assert(func->fn);
	return &func->ast;
}
