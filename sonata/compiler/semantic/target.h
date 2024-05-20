#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

static inline Stmt*
analyze_stmts(Parser* parser, int* count)
{
	*count = 0;
	Stmt* last = NULL;
	list_foreach(&parser->stmt_list.list)
	{
		auto stmt = list_at(Stmt, link);
		if (target_list_has(&stmt->target_list, TARGET_TABLE) ||
		    target_list_has(&stmt->target_list, TARGET_TABLE_REFERENCE))
		{
			last = stmt;
			(*count)++;
		}
	}
	return last;
}
