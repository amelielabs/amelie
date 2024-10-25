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

void os_memusage(uint64_t*, uint64_t*, uint64_t*);
void os_cpuusage_system(int*, uint64_t*);
void os_cpuusage(int, int*, uint64_t*);
