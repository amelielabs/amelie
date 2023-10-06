
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>

void
parser_init(Parser* self)
{
	query_init(&self->query);
	lex_init(&self->lex, keywords);
}

void
parser_free(Parser* self)
{
	query_free(&self->query);
}

void
parser_reset(Parser* self)
{
	lex_reset(&self->lex);
	query_reset(&self->query);
}

static inline bool
if_not_exists(Parser* self)
{
	if (! parser_if(self, KIF))
		return false;
	if (! parser_if(self, KNOT))
		error("IF <NOT> EXISTS expected");
	if (! parser_if(self, KEXISTS))
		error("IF NOT <EXISTS> expected");
	return true;
}

static inline bool
if_exists(Parser* self)
{
	if (! parser_if(self, KIF))
		return false;
	if (! parser_if(self, KEXISTS))
		error("IF <EXISTS> expected");
	return true;
}

static void
parser_create_user(Parser* self)
{
	// CREATE USER [IF NOT EXISTS] name [PASSWORD value]
	auto query = &self->query;
	query_validate(query, false);
	auto stmt = ast_create_user_allocate();
	ast_list_add(&query->stmts, &stmt->ast);
	stmt->config = user_config_allocate();

	// if not exists
	stmt->if_not_exists = if_not_exists(self);

	// name
	auto name = parser_if(self, KNAME);
	if (! name)
		error("CREATE USER <name> expected");
	user_config_set_name(stmt->config, &name->string);

	// PASSWORD
	if (parser_if(self, KPASSWORD))
	{
		// value
		auto value = parser_if(self, KSTRING);
		if (! value)
			error("CREATE USER PASSWORD <value> string expected");
		user_config_set_secret(stmt->config, &value->string);
	}
}

static void
parser_drop_user(Parser* self)
{
	// DROP USER [IF EXISTS] name
	auto query = &self->query;
	query_validate(query, false);
	auto stmt = ast_drop_user_allocate();
	ast_list_add(&query->stmts, &stmt->ast);

	// if exists
	stmt->if_exists = if_exists(self);

	// name
	stmt->name = parser_if(self, KNAME);
	if (! stmt->name)
		error("DROP USER <name> expected");
}

static bool
parser_primary_key(Parser* self)
{
	// PRIMARY KEY
	if (! parser_if(self, KPRIMARY))
		return false;
	if (! parser_if(self, KKEY))
		error("PRIMARY <KEY> expected");
	return true;
}

static void
parser_primary_key_main(Parser* self, AstCreateTable* stmt)
{
	auto schema = &stmt->config->schema;

	// (
	if (! parser_if(self, '('))
		error("KEY <(> expected");

	for (;;)
	{
		// name
		auto name = parser_if(self, KNAME);
		if (! name)
			error("PRIMARY KEY (<name> expected");

		// ensure column exists
		auto column = schema_find_column(schema, &name->string);
		if (! column)
			error("<%.*s> column does not exists", str_size(&name->string),
			      str_of(&name->string));

		// validate column type
		if (column->type != TYPE_INT && column->type != TYPE_STRING)
			error("<%.*s> column key can be string or int", str_size(&name->string),
			      str_of(&name->string));

		// ensure key is not redefined
		auto key = schema_find_key(schema, &name->string);
		if (key)
			error("<%.*s> column redefined as a key", str_size(&name->string),
			      str_of(&name->string));

		// ASC | DESC
		bool asc = true;
		if (parser_if(self, KASC))
			asc = true;
		else
		if (parser_if(self, KDESC))
			asc = false;
		column_set_asc(column, asc);

		// add column as a key
		schema_add_key(schema, column);

		// ,
		if (! parser_if(self, ','))
			break;
	}
}

static int
parser_type(Parser* self, Ast* name)
{
	auto ast = parser_next(self);
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
	case KTFLOAT:
		type = TYPE_FLOAT;
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
parser_schema(Parser* self, AstCreateTable* stmt)
{
	// (name type [primary key], ..., primary key())
	auto schema = &stmt->config->schema;

	// (
	if (! parser_if(self, '('))
		error("CREATE TABLE name <(> expected");

	for (;;)
	{
		// PRIMARY KEY()
		if (parser_primary_key(self))
		{
			parser_primary_key_main(self, stmt);
			goto last;
		}

		// name
		auto name = parser_if(self, KNAME);
		if (! name)
			error("(<name> expected");

		// type
		int type = parser_type(self, name);

		// ensure column does not exists
		auto column = schema_find_column(schema, &name->string);
		if (column)
			error("<%.*s> column redefined", str_size(&name->string),
			      str_of(&name->string));

		// create new column
		column = column_allocate();
		schema_add_column(schema, column);
		column_set_name(column, &name->string);
		column_set_type(column, type);

		if (parser_primary_key(self))
		{
			// validate column type
			if (type != TYPE_INT && type != TYPE_STRING)
				error("<%.*s> column key can be string or int",
				      str_size(&name->string), str_of(&name->string));
			schema_add_key(schema, column);
		}

		// ,
		if (parser_if(self, ','))
			continue;
last:
		// )
		if (parser_if(self, ')'))
			break;

		error("CREATE TABLE name (name TYPE <,)> expected");
	}

	if (schema->key_count == 0)
		error("primary key is not defined");
}

static void
parser_create_table(Parser* self)
{
	// CREATE TABLE [IF NOT EXISTS] name (schema)
	auto query = &self->query;
	query_validate(query, true);

	auto stmt = ast_create_table_allocate();
	ast_list_add(&query->stmts, &stmt->ast);

	// if not exists
	stmt->if_not_exists = if_not_exists(self);

	// create table config
	stmt->config = table_config_allocate();
	Uuid id;
	uuid_mgr_generate(global()->uuid_mgr, &id);
	table_config_set_id(stmt->config, &id);

	// name
	auto name = parser_if(self, KNAME);
	if (! name)
		error("CREATE TABLE <name> expected");
	table_config_set_name(stmt->config, &name->string);

	// (schema)
	parser_schema(self, stmt);
}

static void
parser_drop_table(Parser* self)
{
	// DROP TABLE [IF EXISTS] name
	auto query = &self->query;
	query_validate(query, false);
	auto stmt = ast_drop_table_allocate();
	ast_list_add(&query->stmts, &stmt->ast);

	// if exists
	stmt->if_exists = if_exists(self);

	// name
	stmt->name = parser_if(self, KNAME);
	if (! stmt->name)
		error("DROP TABLE <name> expected");
}

hot void
parser_run(Parser* self, Str* str)
{
	auto query = &self->query;
	lex_start(&self->lex, str);

	bool eof = false;
	while (! eof)
	{
		auto ast = lex_next(&self->lex);
		switch (ast->id) {
		case KBEGIN:
			// BEGIN
			query_validate(query, true);
			if (query->in_transaction)
				error("already in transaction");
			query->in_transaction = true;
			break;
		case KCOMMIT:
			// COMMIT
			query_validate(query, true);
			if (! query->in_transaction)
				error("not in transaction");
			query->complete = true;
			break;
		case KSHOW:
		{
			// SHOW name
			query_validate(query, false);
			auto name = parser_if(self, KNAME);
			if (! name)
				error("SHOW <name> expected");
			auto stmt = ast_show_allocate(name);
			ast_list_add(&query->stmts, &stmt->ast);
			break;
		}
		case KSET:
		{
			// SET name TO INT|STRING
			query_validate(query, false);
			auto name = parser_if(self, KNAME);
			if (! name)
				error("SET <name> expected");
			auto value = lex_next(&self->lex);
			if (value->id != KINT && value->id != KSTRING)
				error("SET name TO <value> expected string or int");
			auto stmt = ast_set_allocate(name, value);
			ast_list_add(&query->stmts, &stmt->ast);
			break;
		}
		case KCREATE:
		{
			// CREATE TABLE | USER
			if (parser_if(self, KUSER))
				parser_create_user(self);
			else
			if (parser_if(self, KTABLE))
				parser_create_table(self);
			else
				error("CREATE <USER|TABLE> expected");
			break;
		}
		case KDROP:
		{
			// DROP TABLE | USER
			if (parser_if(self, KUSER))
				parser_drop_user(self);
			else
			if (parser_if(self, KTABLE))
				parser_drop_table(self);
			else
				error("DROP <USER|TABLE> expected");
			break;
		}
		case KINSERT:
			break;
		case KEOF:
			eof = true;
			break;
		default:
			break;
		}

		// ;
		parser_if(self, ';');
	}
}
