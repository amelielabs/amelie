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

enum
{
	CDC_NONE   = 0,
	CDC_LSN    = 1 << 0,
	CDC_TARGET = 1 << 1
};

void cdc_export(Buf*, Str*, Str*, CdcEvent*, int);
