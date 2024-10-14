#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

enum
{
	AGGR_COUNT,
	AGGR_SUM,
	AGGR_AVG,
	AGGR_MIN,
	AGGR_MAX,
	AGGR_LAMBDA
};

extern AggrIf* aggrs[];
