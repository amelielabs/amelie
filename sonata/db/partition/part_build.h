#pragma once

//
// sonata.
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
	int           column_order;
	Buf*          column_data;
	IndexConfig*  index;
};

static inline void
part_build_init(PartBuild*    self,
                PartBuildType type,
                Part*         part,
                Part*         part_dest,
                int           column_order,
                Buf*          column_data,
                IndexConfig*  index)
{
	self->type         = type;
	self->part         = part;
	self->part_dest    = part_dest;
	self->column_order = column_order;
	self->column_data  = column_data;
	self->index        = index;
}

void part_build(PartBuild*);
