
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>

void
session_execute_import(Session* self)
{
	auto client  = self->client;
	auto request = &client->request;
	auto reply   = &client->reply;
	auto body    = &reply->content;

	// POST /schema/table <&columns=...>
	Uri uri;
	uri_init(&uri);
	guard(uri_free, &uri);
	auto url = &request->options[HTTP_URL];
	uri_set(&uri, url, true);

	Str schema;
	Str name;
	str_init(&schema);
	str_init(&name);

	// Content-Type
	auto type = http_find(request, "Content-Type", 12);
	if (unlikely(! type))
		error("Content-Type is missing");

	if (! str_is(&type->value, "application/json", 16))
		error("unsupported API operation");

	(void)body;
}
