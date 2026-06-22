
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
	// SHOW [ALL] [section] [name] [ON name] [VERBOSE]
	auto stmt = ast_show_allocate();
	self->ast = &stmt->ast;
	self->ret = &stmt->ret;

	// set returning column
	auto column = column_allocate();
	column_set_type(column, TYPE_JSON, -1, 0);
	columns_add(&stmt->ret.columns, column);

	// [ALL]
	stmt->all = stmt_if(self, KALL) != NULL;

	// SHOW ALL
	if (stmt_if(self, KEOF) || stmt_if(self, ';'))
	{
		if (! stmt->all)
			stmt_error(self, NULL, "ALL or name expected");

		// handle as SHOW CONFIG
		str_set(&stmt->section, "config", 6);
		column_set_name(column, &stmt->section);
		stmt->all = false;
		return;
	}

	// section | option name
	auto name = stmt_next_shadow(self);
	if (name->id != KNAME && name->id != KSTRING)
		stmt_error(self, name, "name expected");
	stmt->section = name->string;
	column_set_name(column, &stmt->section);

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

	// [VERBOSE]
	stmt->verbose = stmt_if(self, KVERBOSE) != NULL;
}

Ast*
parse_show_func(Stmt* self, Str* target)
{
	// [ALL]
	auto all = stmt_if(self, KALL) != NULL;

	// section | option name
	auto section = stmt_next_shadow(self);
	if (! (section->id == KNAME || section->id == KSTRING))
	{
		stmt_push(self, section);
		section = ast(KSTRING);
	}
	section->id = KSTRING;
	*target = section->string;

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

	// [VERBOSE]
	auto verbose = ast(KFALSE);
	if (stmt_if(self, KVERBOSE))
		verbose->id = KTRUE;

	// SHOW ALL
	if (all &&
	    str_empty(&section->string) &&
	    str_empty(&name->string) &&
	    str_empty(&on->string))
	{
		// handle as SHOW CONFIG
		str_set(&section->string, "config", 6);
		all = false;
	}

	// show(section, name, on, verbose)
	auto args_list = section;
	section->next = name;
	name->next    = on;
	on->next      = verbose;
	verbose->next = NULL;

	// args(list_head, NULL)
	auto args = ast_args_allocate();
	args->ast.l       = args_list;
	args->ast.r       = NULL;
	args->ast.integer = 4;
	args->constable   = false;

	// func(NULL, args)
	Str fn;
	if (all)
		str_set(&fn, "show_from_all", 13);
	else
		str_set(&fn, "show_from", 9);

	auto func = ast_func_allocate();
	func->fn    = function_mgr_find(share()->function_mgr, &fn);
	func->ast.l = NULL;
	func->ast.r = &args->ast;
	assert(func->fn);
	return &func->ast;
}
