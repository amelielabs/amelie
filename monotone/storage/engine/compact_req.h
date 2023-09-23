#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct CompactReq CompactReq;

typedef enum
{
	COMPACT_UNDEF,
	COMPACT_SPLIT_1,
	COMPACT_SPLIT_2,
	COMPACT_SNAPSHOT
} CompactOp;

struct CompactReq
{
	CompactOp   op;
	int         compression;
	bool        crc;
	Part*       part;
	Part*       l;
	Part*       r;
	HeapCommit* commit;
};

static inline void
compact_req_init(CompactReq* self)
{
	self->op          = COMPACT_UNDEF;
	self->compression = COMPRESSION_OFF;
	self->crc         = false;
	self->part        = NULL;
	self->l           = NULL;
	self->r           = NULL;
	self->commit      = NULL;
}
