
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_key.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static bool
parse_primary_key(Stmt* self)
{
	// PRIMARY KEY
	if (! stmt_if(self, KPRIMARY))
		return false;
	if (! stmt_if(self, KKEY))
		error("PRIMARY <KEY> expected");
	return true;
}

static void
parse_primary_key_def(Stmt* self, AstTableCreate* stmt)
{
	auto key = &stmt->config->key;

	// (
	if (! stmt_if(self, '('))
		error("KEY <(> expected");

	for (;;)
	{
		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("PRIMARY KEY (<name> expected");

		// ensure column exists
		auto column = key_find_column(key, &name->string);
		if (! column)
			error("<%.*s> column does not exists", str_size(&name->string),
			      str_of(&name->string));

		// validate column type
		if (column->type != TYPE_INT && column->type != TYPE_STRING)
			error("<%.*s> column key can be string or int", str_size(&name->string),
			      str_of(&name->string));

		// ensure key is not redefined
		if (key_find_key(key, &name->string))
			error("<%.*s> column redefined as a key", str_size(&name->string),
			      str_of(&name->string));

		// ASC | DESC
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;
		column_set_asc(column, asc);

		// add column as a key
		key_add_key(key, column);

		// ,
		if (! stmt_if(self, ','))
			break;
	}
}

static int
parse_type(Stmt* self, Ast* name)
{
	auto ast = stmt_next(self);
	int  type = 0;
	switch (ast->id) {
	case KTMAP:
		type = TYPE_MAP;
		break;
	case KTARRAY:
		type = TYPE_ARRAY;
		break;
	case KTINT:
		type = TYPE_INT;
		break;
	case KTBOOL:
		type = TYPE_BOOL;
		break;
	case KTREAL:
		type = TYPE_REAL;
		break;
	case KTSTRING:
		type = TYPE_STRING;
		break;
	default:
		error("(%.*s <TYPE>) expected", str_size(&name->string),
		      str_of(&name->string));
		break;
	}
	return type;
}

static void
parser_key(Stmt* self, AstTableCreate* stmt)
{
	// (name type [primary key], ..., primary key())
	auto key = &stmt->config->key;

	// (
	if (! stmt_if(self, '('))
		error("CREATE TABLE name <(> expected");

	for (;;)
	{
		// PRIMARY KEY()
		if (parse_primary_key(self))
		{
			parse_primary_key_def(self, stmt);
			goto last;
		}

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("(<name> expected");

		// type
		int type = parse_type(self, name);

		// ensure column does not exists
		auto column = key_find_column(key, &name->string);
		if (column)
			error("<%.*s> column redefined", str_size(&name->string),
			      str_of(&name->string));

		// create new column
		column = column_allocate();
		key_add_column(key, column);
		column_set_name(column, &name->string);
		column_set_type(column, type);

		if (parse_primary_key(self))
		{
			// validate column type
			if (type != TYPE_INT && type != TYPE_STRING)
				error("<%.*s> column key can be string or int",
				      str_size(&name->string), str_of(&name->string));
			key_add_key(key, column);
		}

		// ,
		if (stmt_if(self, ','))
			continue;
last:
		// )
		if (stmt_if(self, ')'))
			break;

		error("CREATE TABLE name (name TYPE <,)> expected");
	}

	if (key->key_count == 0)
		error("primary key is not defined");
}

void
parse_table_create(Stmt* self)
{
	// CREATE TABLE [IF NOT EXISTS] name (key)
	auto stmt = ast_table_create_allocate();
	self->ast = &stmt->ast;

	// if not exists
	stmt->if_not_exists = parse_if_not_exists(self);

	// create table config
	stmt->config = table_config_allocate();
	Uuid id;
	uuid_mgr_generate(global()->uuid_mgr, &id);
	table_config_set_id(stmt->config, &id);

	// name
	auto name = stmt_if(self, KNAME);
	if (! name)
		error("CREATE TABLE <name> expected");
	table_config_set_name(stmt->config, &name->string);

	// (key)
	parser_key(self, stmt);
}

void
parse_table_drop(Stmt* self)
{
	// DROP TABLE [IF EXISTS] name
	auto stmt = ast_table_drop_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	stmt->name = stmt_if(self, KNAME);
	if (! stmt->name)
		error("DROP TABLE <name> expected");
}

void
parse_table_alter(Stmt* self)
{
	// ALTER TABLE [IF EXISTS] name [RENAME TO]
	unused(self);

	// todo
}
