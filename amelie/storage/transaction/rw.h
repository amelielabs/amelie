#pragma once

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

hot static inline void
row_write(Tr* tr, Row* row)
{
	if (! row->is_heap)
		return;

	// strict serializability.
	//
	// attempt to access row that being previously
	// read or updated by conconcurrent transaction violating
	// serial order
	auto tsn = row_tsn(row);
	if (unlikely(tsn > tr->tsn))
		error("serialization error");

	// keep the maximum access transaction id (for aborts)
	if (tsn > tr->tsn_max)
		tr->tsn_max = tsn;

	// mark row accessed by this transaction id
	row_tsn_set(row, tr->tsn);
}

hot static inline Row*
row_read(Tr* tr, Row* row)
{
	if (! row->is_heap)
		return row;

	// strict serializability.
	//
	// Handling reads as writes, validate serial order and
	// update row tsn.
	//
	// head row is always correct.
	//
	row_write(tr, row);
	return row;
}
