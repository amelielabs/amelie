
//
// sonata.
//
// Real-Time SQL Database.
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
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>

void
parser_init(Parser* self, Db* db)
{
	self->explain = EXPLAIN_NONE;
	self->stmt    = NULL;
	self->db      = db;
	stmt_list_init(&self->stmt_list);
	lex_init(&self->lex, keywords);
}

void
parser_reset(Parser* self)
{
	self->explain = EXPLAIN_NONE;
	self->stmt    = NULL;
	list_foreach_safe(&self->stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		parse_stmt_free(stmt);
	}
	stmt_list_init(&self->stmt_list);
	lex_reset(&self->lex);
}
