#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// tokens and keywords
#include "parser/keyword.h"

// ast
#include "parser/ast.h"
#include "parser/ast_list.h"
#include "parser/ast_stack.h"

// lex
#include "parser/lex.h"

// target
#include "parser/target.h"
#include "parser/target_list.h"
#include "parser/stmt.h"

// parser
#include "parser/parser.h"
#include "parser/parse_call.h"
#include "parser/parse_case.h"
#include "parser/parse_expr.h"
#include "parser/parse_misc.h"
#include "parser/parse_system.h"
#include "parser/parse_config.h"
#include "parser/parse_user.h"
#include "parser/parse_replica.h"
#include "parser/parse_repl.h"
#include "parser/parse_node.h"
#include "parser/parse_schema.h"
#include "parser/parse_table.h"
#include "parser/parse_index.h"
#include "parser/parse_view.h"
#include "parser/parse_cte.h"
#include "parser/parse_insert.h"
#include "parser/parse_update.h"
#include "parser/parse_delete.h"
#include "parser/parse_from.h"
#include "parser/parse_label.h"
#include "parser/parse_aggr.h"
#include "parser/parse_order.h"
#include "parser/parse_group.h"
#include "parser/parse_select.h"
#include "parser/parse_watch.h"
#include "parser/parse.h"
