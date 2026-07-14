
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>

void
cdc_export(Buf* buf, Str* rel_user, Str* rel, CdcEvent* event)
{
	// command (matching log commands)
	Str cmd;
	switch (event->cmd) {
	// write
	case 0:
		str_set(&cmd, "write", 5);
		break;
	// delete
	case 1:
		str_set(&cmd, "delete", 6);
		break;
	// publish
	case 2:
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
