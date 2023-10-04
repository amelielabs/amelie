
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

extern void  parseInit(void*);
extern void *parseAlloc(void *(*)(size_t));
extern void  parse(void*, int, Ast*, Query*);

void
parser_init(Parser* self)
{
	self->parser = NULL;
	query_init(&self->query);
	lex_init(&self->lex, keywords);
}

void
parser_free(Parser* self)
{
	if (self->parser)
	{
		mn_free(self->parser);
		self->parser = NULL;
	}
	query_free(&self->query);
}

void
parser_reset(Parser* self)
{
	// since destructors are not used
	// doing reinit directly
	if (self->parser)
		parseInit(self->parser);
	query_reset(&self->query);
}

void
parser_run(Parser* self, Str* str)
{
	if (self->parser == NULL)
		self->parser = parseAlloc(mn_malloc);
	lex_start(&self->lex, str);
	bool eof = false;
	while (! eof)
	{
		auto ast = lex_next(&self->lex);
		eof = ast->id == 0;
		parse(self->parser, ast->id, ast, &self->query);
	}
}
