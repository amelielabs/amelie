
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>

void
recover(Db* self)
{
	// prepare wal mgr
	wal_open(&self->wal);

	// todo: replay logs

	// start wal mgr
	wal_start(&self->wal);
}
