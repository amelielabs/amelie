#pragma once

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

// tokens and keywords
#include "parser/keyword.h"

// ast
#include "parser/ast.h"
#include "parser/ast_list.h"
#include "parser/ast_stack.h"

// lex
#include "parser/lex.h"

// scope
#include "parser/var.h"
#include "parser/cte.h"
#include "parser/scope.h"

// target
#include "parser/target.h"
#include "parser/targets.h"

// stmt
#include "parser/stmt.h"
#include "parser/stmts.h"
#include "parser/endpoint.h"

// parser
#include "parser/parser.h"
#include "parser/parse.h"

// expr
#include "parser/parse_target.h"
#include "parser/parse_agg.h"
#include "parser/parse_args.h"
#include "parser/parse_func.h"
#include "parser/parse_call.h"
#include "parser/parse_execute.h"
#include "parser/parse_case.h"
#include "parser/parse_expr.h"
#include "parser/parse_encode.h"

// system
#include "parser/parse_system.h"
#include "parser/parse_show.h"
#include "parser/parse_user.h"
#include "parser/parse_token.h"
#include "parser/parse_replica.h"
#include "parser/parse_repl.h"
#include "parser/parse_watch.h"

// ddl
#include "parser/parse_schema.h"
#include "parser/parse_table.h"
#include "parser/parse_index.h"
#include "parser/parse_procedure.h"
#include "parser/parse_cte.h"

// dml
#include "parser/parse_from.h"
#include "parser/parse_returning.h"
#include "parser/parse_row.h"
#include "parser/parse_value.h"
#include "parser/parse_insert.h"
#include "parser/parse_update.h"
#include "parser/parse_delete.h"

// query
#include "parser/parse_order.h"
#include "parser/parse_group.h"
#include "parser/parse_select.h"

// import
#include "parser/parse_endpoint.h"
#include "parser/parse_import.h"
