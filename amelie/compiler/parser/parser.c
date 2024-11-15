
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

void
parser_init(Parser*      self,
            Db*          db,
            FunctionMgr* function_mgr,
            CodeData*    data,
            RowData*     data_row)
{
	self->explain      = EXPLAIN_NONE;
	self->stmt         = NULL;
	self->args         = NULL;
	self->data         = data;
	self->data_row     = data_row;
	self->function_mgr = function_mgr;
	self->local        = NULL;
	self->db           = db;
	stmt_list_init(&self->stmt_list);
	cte_list_init(&self->cte_list);
	lex_init(&self->lex, keywords);
	json_init(&self->json);
}

void
parser_reset(Parser* self)
{
	self->explain = EXPLAIN_NONE;
	self->stmt    = NULL;
	self->args    = NULL;
	self->local   = NULL;
	list_foreach_safe(&self->stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		parse_stmt_free(stmt);
	}
	cte_list_reset(&self->cte_list);
	stmt_list_init(&self->stmt_list);
	lex_reset(&self->lex);
	json_reset(&self->json);
}

void
parser_free(Parser* self)
{
	parser_reset(self);
	json_free(&self->json);
}
