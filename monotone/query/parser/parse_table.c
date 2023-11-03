
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

static int
parse_type(Stmt* self, Ast* path)
{
	auto ast = stmt_next(self);
	int  type = 0;
	switch (ast->id) {
	case KTMAP:
	case KTOBJECT:
		type = TYPE_MAP;
		break;
	case KTARRAY:
		type = TYPE_ARRAY;
		break;
	case KTINT:
	case KTINTEGER:
		type = TYPE_INT;
		break;
	case KTBOOL:
	case KTBOOLEAN:
		type = TYPE_BOOL;
		break;
	case KTREAL:
		type = TYPE_REAL;
		break;
	case KTSTRING:
	case KTEXT:
		type = TYPE_STRING;
		break;
	default:
		error("%.*s <TYPE> expected", str_size(&path->string),
		      str_of(&path->string));
		break;
	}
	return type;
}

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
	auto def = &stmt->config->def;

	// (
	if (! stmt_if(self, '('))
		error("PRIMARY KEY <(> expected");

	for (;;)
	{
		Str     name;
		Str     path;
		Column* column;
		int     type;
		str_init(&name);
		str_init(&path);

		auto tk = stmt_next(self);
		if (tk->id == KNAME)
		{
			// (column, ...)
			str_set_str(&name, &tk->string);

			// find column and validate type
			column = def_find_column(def, &name);
			if (! column)
				error("<%.*s> column does not exists", str_size(&name),
				      str_of(&name));

			type = column->type;
		} else
		if (tk->id == KNAME_COMPOUND)
		{
			// (column.path type, ...)
			str_split_or_set(&tk->string, &name, '.');

			// exclude column name from the path
			str_set_str(&path, &tk->string);
			str_advance(&path, str_size(&name) + 1);

			// find column and validate type
			column = def_find_column(def, &name);
			if (! column)
				error("<%.*s> column does not exists", str_size(&name),
				      str_of(&name));

			// type
			type = parse_type(self, tk);
		} else {
			error("PRIMARY KEY (<name> expected");
		}

		// ASC | DESC
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		// validate key type
		if (type != TYPE_INT && type != TYPE_STRING)
			error("<%.*s> column key can be string or int", str_size(&name),
			      str_of(&name));

		// force column not_null constraint
		constraint_set_not_null(&column->constraint, true);

		// create key
		auto key = key_allocate();
		key_set_column(key, column->order);
		key_set_type(key, type);
		key_set_path(key, &path);
		key_set_asc(key, asc);
		def_add_key(def, key);

		// ,
		if (! stmt_if(self, ','))
			break;
	}

	// )
	if (! stmt_if(self, ')'))
		error("PRIMARY KEY (<)> expected");
}

static void
parse_constraint(Stmt* self, Column* column)
{
	// constraint
	auto cons = &column->constraint;

	// NOT NULL
	if (stmt_if(self, KNOT))
	{
		if (! stmt_if(self, KNULL))
			error("NOT <NULL> expected");
		constraint_set_not_null(cons, true);
	}

	// DEFAULT expr
	if (stmt_if(self, KDEFAULT))
	{
		// const
		char* value_start = self->lex->pos;
		char* value_end;
		auto  value = stmt_next(self);
		value_end = self->lex->pos;
		stmt_push(self, value);

		auto  expr = parse_expr(self, NULL);
		switch (expr->id) {
		case KNULL:
		case KREAL:
		case KINT:
		case KSTRING:
		case KTRUE:
		case KFALSE:
			break;
		default:
			error("only consts allowed as DEFAULT expression");
			break;
		}

		Str string;
		str_init(&string);
		str_set(&string, value_start, value_end - value_start);
		str_shrink(&string);
		constraint_set_default(cons, &string);
	}

	// GENERATED
	if (stmt_if(self, KGENERATED))
	{
		// SERIAL
		if (stmt_if(self, KSERIAL))
		{
			constraint_set_generated(cons, GENERATED_SERIAL);
			constraint_set_not_null(&column->constraint, true);
		} else
		{
			error("unsupported GENERATED expression");
		}
	}
}

static void
parse_def(Stmt* self, AstTableCreate* stmt)
{
	// (name type [not null | default | generated | primary key], ...,
	//  primary key())
	auto def = &stmt->config->def;

	// (
	if (! stmt_if(self, '('))
		error("CREATE TABLE name <(> expected");

	for (;;)
	{
		// PRIMARY KEY ()
		if (parse_primary_key(self))
		{
			// (columns)
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
		auto column = def_find_column(def, &name->string);
		if (column)
			error("<%.*s> column redefined", str_size(&name->string),
			      str_of(&name->string));

		// create column
		column = column_allocate();
		def_add_column(def, column);
		column_set_name(column, &name->string);
		column_set_type(column, type);

		// NOT NULL | DEFAULT | GENERATED
		parse_constraint(self, column);

		// PRIMARY KEY
		if (parse_primary_key(self))
		{
			// force not_null constraint for keys
			constraint_set_not_null(&column->constraint, true);

			// validate key type
			if (column->type != TYPE_INT && column->type != TYPE_STRING)
				error("<%.*s> column key can be string or int",
				      str_size(&column->name), str_of(&column->name));

			// create key
			auto key = key_allocate();
			key_set_column(key, column->order);
			key_set_type(key, column->type);
			def_add_key(def, key);
		}

		// ,
		if (stmt_if(self, ','))
			continue;
last:
		// )
		if (stmt_if(self, ')'))
			break;

		error("CREATE TABLE name (name type <,)> expected");
	}

	if (def->key_count == 0)
		error("primary key is not defined");

}

static void
parse_with(Stmt* self, AstTableCreate* stmt)
{
	// [WITH]
	if (! stmt_if(self, KWITH))
		return;

	// (
	if (! stmt_if(self, '('))
		error("WITH <(> expected");

	for (;;)
	{
		// key
		auto key = stmt_if(self, KNAME);
		if (! key)
			error("WITH (<name> expected");

		// uuid
		if (str_compare_raw(&key->string, "uuid", 4))
		{
			// =
			if (! stmt_if(self, '='))
				error("WITH (uuid <=> expected");

			// string
			auto value = stmt_if(self, KSTRING);
			if (! value)
				error("WITH (uuid = <string>) expected");

			Uuid uuid;
			uuid_from_string(&uuid, &value->string);
			table_config_set_id(stmt->config, &uuid);

		} else {
			error("<%.*s> unrecognized parameter",
			      str_size(&key->string), str_of(&key->string));
		}

		// ,
		if (stmt_if(self, ','))
			continue;

		// )
		if (stmt_if(self, ')'))
			break;
	}
}

static inline void
parse_validate_constraints(AstTableCreate* stmt)
{
	auto def = &stmt->config->def;

	// validate constraints
	auto column = def->column;
	for (; column; column = column->next)
	{
		auto cons = &column->constraint;
		if (cons->generated == GENERATED_NONE)
			continue;

		if (cons->generated == GENERATED_SERIAL)
		{
			auto key = column->key;
			if (! key)
				error("GENERATED SERIAL can be used only with key columns");

			if (column->type != TYPE_INT)
				error("GENERATED SERIAL column <%.*s> must be integer",
				      str_size(&column->name),
				      str_of(&column->name));

			continue;
		}

		if (column->key)
			error("GENERATED columns cannot be used with keys");
	}
}

void
parse_table_create(Stmt* self)
{
	// CREATE TABLE [IF NOT EXISTS] name (key)
	// [REFERENCE] [WITH()]
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
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("CREATE TABLE <name> expected");
	table_config_set_schema(stmt->config, &schema);
	table_config_set_name(stmt->config, &name);

	// (columns)
	parse_def(self, stmt);
	parse_validate_constraints(stmt);

	// [REFERENCE]
	if (stmt_if(self, KREFERENCE))
		table_config_set_reference(stmt->config, true);

	// [WITH]
	parse_with(self, stmt);
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
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("DROP TABLE <name> expected");
}

void
parse_table_alter(Stmt* self)
{
	// ALTER TABLE [IF EXISTS] [schema.]name RENAME [schema.]name
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER TABLE <name> expected");

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER TABLE <RENAME> expected");

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		error("ALTER TABLE RENAME <name> expected");
}
