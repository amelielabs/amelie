#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

enum
{
	AGGR_COUNT,
	AGGR_SUM,
	AGGR_AVG,
	AGGR_MIN,
	AGGR_MAX
};

extern AggrIf *aggrs[];
