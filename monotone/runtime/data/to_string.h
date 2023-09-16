#pragma once

//
// monotone
//
// SQL OLTP database
//

int json_to_string(Buf *data, uint8_t** pos);
int json_to_string_pretty(Buf* data, int deep, uint8_t** pos);
