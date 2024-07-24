#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
};

static inline void
part_build_init(PartBuild*    self,
                PartBuildType type,
                Part*         part,
                Part*         part_dest,
                Column*       column,
                IndexConfig*  index)
{
	self->type      = type;
	self->part      = part;
	self->part_dest = part_dest;
	self->column    = column;
	self->index     = index;
}

void part_build(PartBuild*);
