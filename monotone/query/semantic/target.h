#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline bool
target_list_expr_or_reference(TargetList* self)
{
	auto target = self->list;
	while (target)
	{
		if (target->table && !target->table->config->reference)
			return false;
		target = target->next;
	}
	return true;
}

static inline void
target_list_validate(TargetList* self, Target* primary)
{
	auto table = primary->table;
	assert(table);

	// SELECT FROM distributed table

	// validate supported targets as expression or reference table
	if (unlikely(table->config->reference))
		error("primary distributed table cannot be a reference table");

	auto target = self->list;
	for (; target; target = target->next)
	{
		if (target == primary)
			continue;

		// including views
		if (target->expr)
			continue;

		// skip group by targets
		if (target->group_main)
			continue;

		if (target->table->config->reference)
			continue;

		error("subqueries to distributed tables are not supported");
	}
}
