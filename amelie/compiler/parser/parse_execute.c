
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>

static void
parse_execute_error(Udf* udf)
{
	auto config = udf->config;
	error("%.*s.%.*s(): expected %d arguments",
	      str_size(&config->schema),
	      str_of(&config->schema),
	      str_size(&config->name),
	      str_of(&config->name),
	      config->columns.list_count);
}

hot static inline void
parse_arg(Stmt* self, Column* column)
{
	// get the begining of current text position
	auto lex = self->lex;
	auto pos = lex->pos;
	while (lex->backlog)
	{
		pos = lex->backlog->pos;
		lex->backlog = lex->backlog->prev;
	}

	// parse and encode json value
	json_reset(self->json);
	Str in;
	str_set(&in, pos, self->lex->end - pos);
	json_set_time(self->json, self->local->timezone, self->local->time_us);
	json_parse(self->json, &in, &self->data->data);
	self->lex->pos = self->json->pos;

	// validate column type
	(void)column;
}

void
parse_execute(Stmt* self)
{
	// EXECUTE [schema.]name ([args])
	auto stmt = ast_execute_allocate();
	self->ast = &stmt->ast;

	// name
	Str schema;
	Str name;
	if (! parse_target(self, &schema, &name))
		error("EXECUTE <name> expected");

	// find function
	stmt->udf = udf_mgr_find(&self->db->udf_mgr, &schema, &name, true);
	auto config = stmt->udf->config;

	// (
	if (! stmt_if(self, '('))
		error("EXECUTE name <(> expected");

	// [
	auto data = &self->data->data;
	encode_array(data);

	// value, ...
	auto columns = &config->columns;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);

		// parse and encode json value
		parse_arg(self, column);

		// ,
		if (stmt_if(self, ','))
		{
			if (list_is_last(&columns->list, &column->link))
				parse_execute_error(stmt->udf);
			continue;
		}
	}

	// ]
	encode_array_end(data);

	// )
	if (! stmt_if(self, ')'))
		parse_execute_error(stmt->udf);
}
