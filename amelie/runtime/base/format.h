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

int format(char*, int, const char*, ...);
int formatv(char*, int, const char*, va_list);
int buf_format(Buf*, const char*, ...);
int buf_formatv(Buf*, const char*, va_list);
