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

void uri_parse(Endpoint*, Str*);
void uri_parse_endpoint(Endpoint*, Str*);
void uri_export(Endpoint*, Buf*);
void uri_export_arg(Opt*, Buf*, bool*);
