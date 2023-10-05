
//
// monotone
//
// SQL OLTP database
//

%include
{
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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-variable"
}

%name parse
%extra_argument { Query* arg }

// fixed stack size to avoid allocations
%stack_size 1000

// non-terminal type
%default_type { Ast* }

// terminal type
%token_type { Ast* }
%token_prefix K

%token COMMA.
%token COLON.
%token SEMICOLON.

%token BEGIN COMMIT ROLLBACK SHOW.

%token TRUE FALSE.
%token NULL.
%token ARGUMENT.
%token ARGUMENT_NAME.
%token NAME.
%token NAME_COMPOUND.
%token NAME_COMPOUND_STAR.
%token STAR.
%token IDX.
%token NEG.

%token EXPR.

%token CREATE USER.
%token CREATE_USER.
%token IF.
%token NOT.
%token EXISTS.
%token DROP.
%token DROP_USER.

%token CREATE_TABLE.
%token TABLE.
%token DROP_TABLE.

// operators
%left  OR.
%left  AND.
%left  EQ  NEQ.
%left  GTE LTE GT LT.
// is, isnull, notnull
// in, between, overlaps, like
%left  BOR.
%left  XOR.
%left  BAND.
%left  SHL SHR.
%left  ADD SUB CAT.
%left  MUL DIV MOD.
// exponentation
%right NOT.
%left  RBL RBR CBL CBR SBL SBR DOT.

///////////////////////////////////////////////////////////////////////////////

program   ::= stmts.
stmts     ::= stmts SEMICOLON stmt.
stmts     ::= stmt.

// STATEMENT
stmt      ::= .
stmt      ::= begin.
stmt      ::= commit.
stmt      ::= rollback.
stmt      ::= show.
stmt      ::= set.
stmt      ::= create_user.
stmt      ::= create_table.
stmt      ::= drop_user.
stmt      ::= drop_table.
stmt      ::= insert.
stmt      ::= select.

// BEGIN, COMMIT, ROLLBACK
begin     ::= BEGIN.
{
	query_validate(arg, true);
	if (arg->in_transaction)
		error("already in transaction");
	arg->in_transaction = true;
}

commit    ::= COMMIT.
{
	query_validate(arg, true);
	if (! arg->in_transaction)
		error("not in transaction");
	arg->complete = true;
}

rollback  ::= ROLLBACK.
{
	query_validate(arg, true);
	if (! arg->in_transaction)
		error("not in transaction");
	arg->complete = true;
}

// SHOW
show      ::= SHOW NAME|STRING(A).
{
	query_validate(arg, false);
	ast_list_add(&arg->stmts, ast_show_allocate(A));
}

// SET
set       ::= SET STRING|NAME(A) TO INT|STRING|TRUE|FALSE(B).
{
	query_validate(arg, false);
	ast_list_add(&arg->stmts, ast_set_allocate(A, B));
}

///////////////////////////////////////////////////////////////////////////////

// CREATE USER
if_exists(A) ::= IF(o) EXISTS. { A = o; }
if_exists(A) ::= . { A = NULL; }

if_not_exists(A) ::= IF(o) NOT EXISTS. { A = o; }
if_not_exists(A) ::= . { A = NULL; }

create_user ::= CREATE USER if_not_exists(A) NAME(B) password(C).
{
	query_validate(arg, false);

	auto ast  = ast_create_user_allocate();
	auto self = ast_create_user_of(ast);
	ast_list_add(&arg->stmts, ast);
	self->config = user_config_allocate();
	// if not exists
	if (A)
		self->if_not_exists = true;
	// name
	user_config_set_name(self->config, &B->string);
	// password
	if (C)
		user_config_set_secret(self->config, &C->string);
}

password(A) ::= PASSWORD STRING(B). { A = B; }
password(A) ::= . { A = NULL; }

// DROP USER
drop_user ::= DROP USER if_exists(A) NAME(B).
{
	query_validate(arg, false);

	auto ast  = ast_drop_user_allocate();
	auto self = ast_drop_user_of(ast);
	ast_list_add(&arg->stmts, ast);
	// if exists
	if (A)
		self->if_exists = true;
	// name
	self->name = B;
}

///////////////////////////////////////////////////////////////////////////////

// CREATE TABLE
create_table ::= CREATE TABLE if_not_exists(A) NAME(B) RBL schema_start schema RBR.
{
	auto ast  = ast_list_last(&arg->stmts);
	auto self = ast_create_table_of(ast);

	if (self->config->schema.key_count == 0)
		error("primary key is not defined");

	// if not exists
	if (A)
		self->if_not_exists = true;
	// name
	table_config_set_name(self->config, &B->string);
}

schema_start ::= .
{
	query_validate(arg, true);

	// create table config
	auto ast  = ast_create_table_allocate();
	auto self = ast_create_table_of(ast);
	ast_list_add(&arg->stmts, ast);
	self->config = table_config_allocate();
	Uuid id;
	uuid_mgr_generate(global()->uuid_mgr, &id);
	table_config_set_id(self->config, &id);
}

schema       ::= schema COMMA column.
schema       ::= schema COMMA primary_key.
schema       ::= column.
column       ::= NAME(A) type(B) type_pk(C).
{
	auto ast    = ast_list_last(&arg->stmts);
	auto self   = ast_create_table_of(ast);
	auto schema = &self->config->schema;

	// ensure column does not exists
	auto column = schema_find_column(&self->config->schema, &A->string);
	if (column)
		error("<%.*s> column redefined", str_size(&A->string),
		      str_of(&A->string));

	int type = B->integer;

	// create new column
	column = column_allocate();
	schema_add_column(schema, column);
	column_set_name(column, &A->string);
	column_set_type(column, type);

	// PRIMARY KEY
	if (C)
	{
		// validate column type
		if (type != TYPE_INT && type != TYPE_STRING)
			error("<%.*s> column key can be string or int",
			      str_size(&A->string), str_of(&A->string));
		schema_add_key(schema, column);
	}
}

type(A)     ::= TMAP(B).        { A = B; A->integer = TYPE_MAP;    }
type(A)     ::= TARRAY(B).      { A = B; A->integer = TYPE_ARRAY;  }
type(A)     ::= TINT(B).        { A = B; A->integer = TYPE_INT;    }
type(A)     ::= TSTRING(B).     { A = B; A->integer = TYPE_STRING; }
type(A)     ::= TBOOL(B).       { A = B; A->integer = TYPE_BOOL;   }
type(A)     ::= TFLOAT(B).      { A = B; A->integer = TYPE_FLOAT;  }
type_pk(A)  ::= PRIMARY(o) KEY. { A = o;                           }
type_pk(A)  ::= .               { A = NULL;                        }

primary_key ::= PRIMARY KEY RBL primary_key_arg RBR.
primary_key_arg ::= primary_key_column.
primary_key_arg ::= primary_key_arg COMMA primary_key_column.
primary_key_column ::= NAME(A) asc(B).
{
	auto ast    = ast_list_last(&arg->stmts);
	auto self   = ast_create_table_of(ast);
	auto schema = &self->config->schema;

	// ensure column exists
	auto column = schema_find_column(&self->config->schema, &A->string);
	if (! column)
		error("<%.*s> column does not exists", str_size(&A->string),
		      str_of(&A->string));

	// validate column type
	if (column->type != TYPE_INT && column->type != TYPE_STRING)
		error("<%.*s> column key can be string or int", str_size(&A->string),
		      str_of(&A->string));

	// ensure key is not redefined
	auto key = schema_find_key(schema, &A->string);
	if (key)
		error("<%.*s> column redefined as a key", str_size(&A->string),
		      str_of(&A->string));

	// ASC | DESC
	bool asc = true;
	if (B && B->id == KDESC)
		asc = false;
	column_set_asc(column, asc);

	// add column as a key
	schema_add_key(schema, column);
}

asc(A) ::= ASC|DESC(o). { A = o; }
asc(A) ::= . { A = NULL; }

// DROP TABLE
drop_table ::= DROP TABLE if_exists(A) NAME(B).
{
	query_validate(arg, false);

	auto ast  = ast_drop_table_allocate();
	auto self = ast_drop_table_of(ast);
	ast_list_add(&arg->stmts, ast);
	// if exists
	if (A)
		self->if_exists = true;
	// name
	self->name = B;
}

///////////////////////////////////////////////////////////////////////////////

// INSERT/REPLACE
insert ::= INSERT|REPLACE INTO NAME(A) VALUES insert_start row_list.
{
	query_validate(arg, true);

	auto ast  = ast_insert_allocate();
	auto self = ast_insert_of(ast);
	ast_list_add(&arg->stmts, ast);

	// name
	self->table = A;

	// values/rows (list of expr_list's)
	self->rows_count = cardinality_pop(&arg->cardinality);
	self->rows = arg->stack.list;
	ast_stack_init(&arg->stack);
}

insert_start ::= .
{
	cardinality_push(&arg->cardinality);
}

// (expr, expr), ()
row_list   ::= row_list COMMA row.
row_list   ::= row.
row        ::= RBL row_start(A) row_arg RBR.
{
	// list of exprs
	int count = cardinality_pop(&arg->cardinality);
	auto list = ast_slice(&arg->stack, A);

	// row(expr, expr, expr)
	auto self = ast_row_allocate(KRBL, list, count);
	ast_push(&arg->stack, self);

	// rows++
	cardinality_inc(&arg->cardinality);
}

row_start(A) ::= .
{
	cardinality_push(&arg->cardinality);
	A = ast_tail(&arg->stack);
}

row_arg    ::= row_arg COMMA row_value.
row_arg    ::= row_value.
row_value  ::= expr_arg(A).
{
	// fold expression
	cardinality_inc(&arg->cardinality);
	ast_push(&arg->stack, A);
}

///////////////////////////////////////////////////////////////////////////////

// SELECT
select ::= SELECT expr.
{
	query_validate(arg, true);

	auto ast  = ast_select_allocate();
	auto self = ast_select_of(ast);
	ast_list_add(&arg->stmts, ast);

	// expr
	self->expr = arg->stack.list;
	ast_stack_init(&arg->stack);
}

///////////////////////////////////////////////////////////////////////////////

// EXPRESSION ARGUMENT

expr_arg(A) ::= expr_start(B) expr.
{
	auto list = ast_slice(&arg->stack, B);
	A = ast_expr_allocate(KEXPR, list);
}

expr_start(A) ::= .                             { A = ast_tail(&arg->stack); }

// EXPRESSION
expr        ::= expr OR(o) expr.                { ast_push(&arg->stack, o); }
expr        ::= expr AND(o) expr.               { ast_push(&arg->stack, o); }
expr        ::= expr EQ|NEQ(o) expr.            { ast_push(&arg->stack, o); }
expr        ::= expr GTE|GT|LTE|LT(o) expr.     { ast_push(&arg->stack, o); }
expr        ::= expr BOR(o) expr.               { ast_push(&arg->stack, o); }
expr        ::= expr XOR(o) expr.               { ast_push(&arg->stack, o); }
expr        ::= expr BAND(o) expr.              { ast_push(&arg->stack, o); }
expr        ::= expr SHL|SHR(o) expr.           { ast_push(&arg->stack, o); }
expr        ::= expr ADD|SUB|CAT(o) expr.       { ast_push(&arg->stack, o); }
expr        ::= expr MUL|DIV|MOD(o) expr.       { ast_push(&arg->stack, o); }
expr        ::= expr SBL(o) expr SBR.           { ast_push(&arg->stack, o); o->id = KIDX; }
expr        ::= expr DOT(o) expr.               { ast_push(&arg->stack, o); }
expr        ::= SUB(o) expr.                    { ast_push(&arg->stack, o); o->id = KNEG; }
expr        ::= NOT(o) expr.                    { ast_push(&arg->stack, o); }
expr        ::= value.

// VALUE

// function call
value       ::= call.

// ({[
value       ::= RBL expr RBR.
value       ::= map.
value       ::= array.

// select
//value      ::= SELECT.

// consts
value       ::= TRUE(A).                       { ast_push(&arg->stack, A); }
value       ::= FALSE(A).                      { ast_push(&arg->stack, A); }
value       ::= NULL(A).                       { ast_push(&arg->stack, A); }
value       ::= INT|STRING|FLOAT(A).           { ast_push(&arg->stack, A); }
value       ::= ARGUMENT|ARGUMENT_NAME(A).     { ast_push(&arg->stack, A); }

// variables
value       ::= NAME(A).                       { ast_push(&arg->stack, A); }
value       ::= NAME_COMPOUND(A).              { ast_push(&arg->stack, A); }
value       ::= NAME_COMPOUND_STAR(A).         { ast_push(&arg->stack, A); }

// *
value       ::= MUL(A).                        { ast_push(&arg->stack, A); A->id = KSTAR; }

// CALL
//
// function_name([expr, ...])
//
call        ::= NAME(B) RBL call_start call_arg RBR.
{
	int count = cardinality_pop(&arg->cardinality);
	auto self = ast_call_allocate(KRBL, B, count);
	ast_push(&arg->stack, self);
}

call_start  ::= .
{
	cardinality_push(&arg->cardinality);
}

call_arg    ::= call_arg COMMA call_value.
call_arg    ::= call_value.
call_arg    ::= .
call_value  ::= expr.
{
	cardinality_inc(&arg->cardinality);
}

// ARRAY
//
// [expr, expr, ...]
//
array       ::= SBL array_start array_arg SBR.
{
	int count = cardinality_pop(&arg->cardinality);
	auto self = ast_call_allocate(KSBL, NULL, count);
	ast_push(&arg->stack, self);
}

array_start ::= .
{
	cardinality_push(&arg->cardinality);
}

array_arg   ::= array_arg COMMA array_value.
array_arg   ::= array_value.
array_arg   ::= .
array_value ::= expr.
{
	cardinality_inc(&arg->cardinality);
}

// MAP
//
// {"string": expr, ...}
//
map         ::= CBL map_start map_arg CBR.
{
	int count = cardinality_pop(&arg->cardinality);
	auto self = ast_call_allocate(KCBL, NULL, count);
	ast_push(&arg->stack, self);
}

map_start   ::= .
{
	cardinality_push(&arg->cardinality);
}

map_arg     ::= map_arg COMMA map_value.
map_arg     ::= map_value.
map_arg     ::= .
map_value   ::= STRING(o) COLON expr.
{
	ast_push(&arg->stack, o);
	cardinality_inc(&arg->cardinality);
}

%syntax_error   { error("syntax error"); }
%parse_failure  { error("parser error"); }
%stack_overflow { error("parser stack overflow"); }

%code
{
#pragma clang diagnostic pop
}
