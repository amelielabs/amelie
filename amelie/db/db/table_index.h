#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

bool table_index_create(Table*, Tr*, IndexConfig*, bool);
void table_index_drop(Table*, Tr*, Str*, bool);
void table_index_rename(Table*, Tr*, Str*, Str*, bool);
