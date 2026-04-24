
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_cdc.h>

void
cdc_export(Buf* buf, Str* rel_user, Str* rel, CdcEvent* event)
{
	(void)rel_user;

	// cmd
	Str cmd;
	switch (event->cmd) {
	case CMD_REPLACE:
		str_set(&cmd, "write", 5);
		break;
	case CMD_DELETE:
		str_set(&cmd, "delete", 6);
		break;
	case CMD_PUBLISH:
		str_set(&cmd, "publish", 7);
		break;
	}
	char fmt[] =
		"\n"
		"{ \"jsonrpc\": \"2.0\", "
		   "\"method\": \"cdc\", "
		   "\"params\": { "
			"\"lsn\": %" PRIu64 ", "
			"\"lsn_op\": %" PRIu32 ", "
			"\"cmd\": \"%.*s\", "
			"\"rel\": \"%.*s\", "
			"\"row\": ";
	buf_printf(buf, fmt, event->lsn, event->lsn_op,
	           str_size(&cmd),
	           str_of(&cmd),
	           str_size(rel),
	           str_of(rel));
	uint8_t* pos = event->data;
	json_export(buf, runtime()->timezone, &pos);
	buf_write(buf, "} }", 3);
}
