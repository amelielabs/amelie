
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

#include <amelie_runtime.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_frontend.h>

void
frontend_native(Frontend* self, Native* native)
{
	auto ctl = self->iface;
	auto session = self->iface->session_create(self, self->iface_arg);
	defer(ctl->session_free, session);

	Str url;
	Str content_type;
	str_set_cstr(&url, "/");
	str_set_cstr(&content_type, "text/plain");

	Content output;
	content_init(&output);
	for (auto connected = true; connected;)
	{
		auto req = request_queue_pop(&native->queue);
		assert(req);
		auto error = false;
		switch (req->type) {
		case REQUEST_CONNECT:
			break;
		case REQUEST_DISCONNECT:
			connected = false;
			break;
		case REQUEST_EXECUTE:
		{
			content_set(&output, &req->content);
			content_reset(&output);
			error = ctl->session_execute(session, &url, &req->cmd, &content_type, &output);
			break;
		}
		}
		request_complete(req, error);
	}
}
