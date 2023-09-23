#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Storage Storage;

struct Storage
{
	int            refs;
	Engine         engine;
	StorageConfig* config;
};

Storage*
storage_create(StorageConfig*, CompactMgr*);
void storage_ref(Storage*);
void storage_unref(Storage*);
void storage_open(Storage*, bool);
void storage_write(Storage*, Transaction*, LogCmd, bool, uint8_t*, int);
Iterator*
storage_read(Storage*, Str*);
