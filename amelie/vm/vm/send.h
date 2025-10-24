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

typedef struct Send Send;

struct Send
{
	int    start;
	bool   has_result; 
	Table* table;
};

static inline Send*
send_create(CodeData* data, int* offset)
{
	*offset = code_data_pos(data);
	auto send = (Send*)buf_claim(&data->data, sizeof(Send));
	return send;
}

static inline Send*
send_at(CodeData* data, int offset)
{
	return (Send*)code_data_at(data, offset);
}
