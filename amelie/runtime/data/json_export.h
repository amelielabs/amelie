#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

void json_export(Buf*, Timezone*, uint8_t**);
void json_export_pretty(Buf*, Timezone*, uint8_t**);
