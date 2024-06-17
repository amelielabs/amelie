
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
	bool create_hash = (self->config->type == INDEX_HASH);

	auto it = index_open(primary, NULL, true);
	guard(iterator_close, it);
	while (iterator_has(it))
	{
		auto row_primary = iterator_at(it);
		auto row = row_create_secondary(&self->config->keys, create_hash, row_primary);
		auto prev = index_ingest(self, row);
		if (unlikely(prev))
		{
			row_free(row);
			error("index unique constraint violation");
		}
		iterator_next(it);
	}
}
