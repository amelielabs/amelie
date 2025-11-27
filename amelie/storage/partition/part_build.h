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

typedef struct PartBuild PartBuild;

typedef enum
{
	PART_BUILD_INDEX,
	PART_BUILD_COLUMN_ADD,
	PART_BUILD_COLUMN_DROP
} PartBuildType;

struct PartBuild
{
	PartBuildType type;
	Part*         part;
	Part*         part_dest;
	Column*       column;
	IndexConfig*  index;
	Str*          schema;
	Str*          name;
};

static inline void
part_build_init(PartBuild*    self,
                PartBuildType type,
                Part*         part,
                Part*         part_dest,
                Column*       column,
                IndexConfig*  index,
                Str*          schema,
                Str*          name)
{
	self->type      = type;
	self->part      = part;
	self->part_dest = part_dest;
	self->column    = column;
	self->index     = index;
	self->schema    = schema;
	self->name      = name;
}

void part_build(PartBuild*);
