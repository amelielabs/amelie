
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
	// cmd
	Str cmd;
	switch (event->cmd) {
	case CDC_WRITE:
		str_set(&cmd, "write", 5);
		break;
	case CDC_DELETE:
		str_set(&cmd, "delete", 6);
		break;
	case CDC_PUBLISH:
		str_set(&cmd, "publish", 7);
		break;
	case CDC_REQUEST:
		str_set(&cmd, "request", 7);
		break;
	}
	buf_format(buf, "{{\"cmd\": \"{str}\", ", &cmd, rel_user);

	// target
	if (str_empty(rel))
		buf_format(buf, "\"target\": \"{str}\", ", rel_user);
	else
		buf_format(buf, "\"target\": \"{str}.{str}\", ", rel_user, rel);

	// [lsn]
	if (event->lsn > 0)
		buf_format(buf, "\"lsn\": {u64}, ", event->lsn);

	// data
	buf_write(buf, "\"data\": ", 8);
	uint8_t* pos = event->data;
	json_export(buf, runtime()->timezone, &pos);
	buf_write(buf, "}", 1);
}
