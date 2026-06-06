
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
		"{{\"jsonrpc\": \"2.0\", "
		  "\"method\": \"event\", "
		  "\"params\": {{"
			"\"lsn\": {u64}, "
			"\"lsn_op\": {u32}, "
			"\"cmd\": \"{str}\", "
			"\"user\": \"{str}\", "
			"\"name\": \"{str}\", "
			"\"row\": ";
	buf_format(buf, fmt, event->lsn, event->lsn_op,
	           &cmd, rel_user, rel);
	uint8_t* pos = event->data;
	json_export(buf, runtime()->timezone, &pos);
	buf_write(buf, "}}", 2);
}
