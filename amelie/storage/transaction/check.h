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

static inline void
check_permission_on(Grants* self, uint32_t perm)
{
	if (! grants_check_first(self, perm))
		error("{s}: permission denied", permission_name_of(perm));
}

static inline void
check_permission(Tr* self, uint32_t perm)
{
	if (self->grants)
		check_permission_on(self->grants, perm);
}
