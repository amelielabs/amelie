
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

static int
parse_type(Stmt* self, Column* column, Str* path)
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
		if (column && path)
			error("%.*s.%.*s <TYPE> expected",
			      str_size(&column->name),
			      str_of(&column->name),
			      str_size(path),
			      str_of(path));
		else
			error("%.*s <TYPE> expected",
			      str_size(&column->name),
			      str_of(&column->name));
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

static Column*
parse_primary_key_column(Stmt* self, Def* def, Str* path)
{
	Column* column = NULL;

	Str     name;
	str_init(&name);

	auto tk = stmt_next(self);
	if (tk->id == KNAME)
	{
		// column
		str_set_str(&name, &tk->string);

	} else
	if (tk->id == KNAME_COMPOUND)
	{
		// column.path
		str_split_or_set(&tk->string, &name, '.');

		// exclude column name from the path
		str_set_str(path, &tk->string);
		str_advance(path, str_size(&name) + 1);

	} else {
		return NULL;
	}

	// find column
	column = def_find_column(def, &name);
	if (! column)
		error("<%.*s> column does not exists", str_size(&name),
		      str_of(&name));

	return column;
}

static void
parse_primary_key_def(Stmt* self, Def* def)
{
	// (
	if (! stmt_if(self, '('))
		error("PRIMARY KEY <(> expected");

	for (;;)
	{
		Str path;
		str_init(&path);

		// (column, ...)
		// (column.path type, ...)
		auto column = parse_primary_key_column(self, def, &path);
		if (column == NULL)
			error("PRIMARY KEY (<name> expected");

		// [type]
		int type;
		if (str_empty(&path))
			type = column->type;
		else
			type = parse_type(self, column, &path);

		// ASC | DESC
		bool asc = true;
		if (stmt_if(self, KASC))
			asc = true;
		else
		if (stmt_if(self, KDESC))
			asc = false;

		// validate key type
		if (type != TYPE_INT && type != TYPE_STRING)
			error("<%.*s> column key can be string or int", str_size(&column->name),
			      str_of(&column->name));

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
parse_constraint(Stmt* self, Def* def, Column* column)
{
	// constraint
	auto cons = &column->constraint;

	bool primary_key = false;
	bool done = false;
	while (! done)
	{
		auto name = stmt_next(self);
		switch (name->id) {
		// NOT NULL
		case KNOT:
		{
			if (! stmt_if(self, KNULL))
				error("NOT <NULL> expected");
			constraint_set_not_null(cons, true);
			break;
		}

		// DEFAULT expr
		case KDEFAULT:
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
			break;
		}

		// SERIAL
		case KSERIAL:
		{
			constraint_set_generated(cons, GENERATED_SERIAL);
			constraint_set_not_null(&column->constraint, true);
			break;
		}

		// GENERATED
		case KGENERATED:
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
			break;
		}

		// PRIMARY KEY
		case KPRIMARY:
		{
			if (! stmt_if(self, KKEY))
				error("PRIMARY <KEY> expected");

			if (primary_key)
				error("PRIMARY KEY defined twice");

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

			primary_key = true;
			break;
		}

		default:
			stmt_push(self, name);
			done = true;
			break;
		}
	}
}

static void
parse_def(Stmt* self, Def* def)
{
	// (name type [not null | default | serial | generated | primary key], ...,
	//  primary key())

	// (
	if (! stmt_if(self, '('))
		error("CREATE TABLE name <(> expected");

	for (;;)
	{
		// PRIMARY KEY ()
		if (parse_primary_key(self))
		{
			// (columns)
			parse_primary_key_def(self, def);
			goto last;
		}

		// name
		auto name = stmt_if(self, KNAME);
		if (! name)
			error("(<name> expected");

		// ensure column does not exists
		auto column = def_find_column(def, &name->string);
		if (column)
			error("<%.*s> column redefined", str_size(&name->string),
			      str_of(&name->string));

		// create column
		column = column_allocate();
		def_add_column(def, column);
		column_set_name(column, &name->string);

		// type
		int type = parse_type(self, column, NULL);
		column_set_type(column, type);

		// PRIMARY KEY | NOT NULL | DEFAULT | SERIAL | GENERATED
		parse_constraint(self, def, column);

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
parse_validate_constraints(Def* def)
{
	// validate constraints
	auto column = def->column;
	for (; column; column = column->next)
	{
		auto cons = &column->constraint;
		if (cons->generated == GENERATED_NONE)
			continue;

		if (cons->generated == GENERATED_SERIAL)
		{
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

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("CREATE TABLE <name> expected");

	// create table config
	stmt->config = table_config_allocate();
	Uuid id;
	uuid_generate(&id, global()->random);
	table_config_set_id(stmt->config, &id);
	table_config_set_schema(stmt->config, &schema);
	table_config_set_name(stmt->config, &name);

	// create primary index config
	auto index_config = index_config_allocate();
	auto def = &index_config->def;
	table_config_add_index(stmt->config, index_config);

	Str index_name;
	str_set_cstr(&index_name, "primary");
	index_config_set_name(index_config, &index_name);
	index_config_set_type(index_config, INDEX_TREE);
	index_config_set_primary(index_config, true);
	def_set_reserved(def, 0);

	// (columns)
	parse_def(self, def);
	parse_validate_constraints(def);

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
	// ALTER TABLE [IF EXISTS] [schema.]name SET SERIAL TO value
	auto stmt = ast_table_alter_allocate();
	self->ast = &stmt->ast;

	// if exists
	stmt->if_exists = parse_if_exists(self);

	// name
	if (! parse_target(self, &stmt->schema, &stmt->name))
		error("ALTER TABLE <name> expected");

	// [SET]
	if (stmt_if(self, KSET))
	{
		// SERIAL
		if (! stmt_if(self, KSERIAL))
			error("ALTER TABLE SET <SERIAL> expected");

		// TO
		if (! stmt_if(self, KTO))
			error("ALTER TABLE SET SERIAL <TO> expected");

		// int
		stmt->serial = stmt_if(self, KINT);
		if (! stmt->serial)
			error("ALTER TABLE SET SERIAL TO <integer> expected");

		return;
	}

	// RENAME
	if (! stmt_if(self, KRENAME))
		error("ALTER TABLE <RENAME> expected");

	// [TO]
	stmt_if(self, KTO);

	// name
	if (! parse_target(self, &stmt->schema_new, &stmt->name_new))
		error("ALTER TABLE RENAME <name> expected");
}
