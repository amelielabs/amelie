
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

#if 0
static inline void
stmt_update(Cmd* cmd)
{
	/* UPDATE name [SET|UNSET] path = expr [, ... ] [WHERE expr] [LIMIT expr] [OFFSET expr] */

	/* table_name, expression or join */
	int level = target_list_next_level(&cmd->target_list);

	target_t *target;
	target = stmt_from(cmd, level);
	table_t *table = target->table;
	if (table == NULL)
		cmd_error(cmd, "UPDATE <table name> expected");
	if (! table_is_primary(target->table))
		cmd_error(cmd, "UPDATE table primary index expected");

	ast_t *expr_update = NULL;
	ast_t *expr_prev   = NULL;
	int       count = 0;
	for (;;)
	{
		/* [SET | UNSET] */
		token_t op;
		int rc;
		rc = cmd_next(cmd, &op);
		switch (rc) {
		case KSET:
		case KUNSET:
			break;
		default:
			cmd_push(cmd, &op);
			op.type = KSET;
			break;
		}

		/* name, path or column id */
		token_t path;
		rc = cmd_next(cmd, &path);
		switch (rc) {
		case LEX_NAME:
		case LEX_NAME_COMPOUND:
		case LEX_INT:
			break;
		default:
			cmd_error(cmd, "UPDATE name <name or pos> expected");
		}

		ast_t *expr = NULL;
		if (op.type == KSET)
		{
			/* [SET] <path> [=] <expr> */

			/* = */
			cmd_try(cmd, NULL, '=');
			
			/* expr */
			expr = expr_parse(cmd, NULL);
		}

		/* expr */
		/* op(path, expr) */
		ast_t *path_expr = ast(cmd, &path, NULL, NULL);
		ast_t *op_expr = ast(cmd, &op, path_expr, expr);
		if (expr_update == NULL)
			expr_update = op_expr;
		else
			expr_prev->next = op_expr;
		expr_prev = op_expr;
		count++;

		/* , */
		rc = cmd_try(cmd, NULL, ',');
		if (! rc)
			break;
	}

	/* [WHERE] */
	ast_t *expr_where = NULL;
	if (cmd_try(cmd, NULL, KWHERE))
		expr_where = expr_parse(cmd, NULL);

	/* combine join on and where expression */
	expr_where = stmt_from_joon_and_where(cmd, target, expr_where);

	/* [LIMIT] */
	ast_t *expr_limit = NULL;
	if (cmd_try(cmd, NULL, KLIMIT))
		expr_limit = expr_parse(cmd, NULL);

	/* [OFFSET] */
	ast_t *expr_offset = NULL;
	if (cmd_try(cmd, NULL, KOFFSET))
		expr_offset = expr_parse(cmd, NULL);

	/* scan */
	update_t update =
	{
		.target      = target,
		.expr_update = expr_update,
		.table       = table
	};
	scan(cmd, target, expr_limit, expr_offset, expr_where,
	        NULL,
	        stmt_update_on_match, &update);
}

#endif

hot void
parse_update(Stmt* self)
{
	// UPDATE name [SET|UNSET] path = expr [, ... ]
	// [WHERE expr]
	// [LIMIT expr]
	// [OFFSET expr]
	auto stmt = ast_update_allocate();
	self->ast = &stmt->ast;

}
