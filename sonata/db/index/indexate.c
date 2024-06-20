
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
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>

hot void
indexate(Index* self, Index* primary)
{
	auto it = index_open(primary, NULL, true);
	guard(iterator_close, it);
	while (iterator_has(it))
	{
		auto row = iterator_at(it);
		auto prev = index_ingest(self, row);
		if (unlikely(prev))
			error("index unique constraint violation");
		iterator_next(it);
	}
}
