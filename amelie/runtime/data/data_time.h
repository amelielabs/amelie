#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

// interval

always_inline hot static inline int
data_size_interval(Interval* iv)
{
	return data_size_type() +
	       data_size_integer(iv->m) +
	       data_size_integer(iv->d) +
	       data_size_integer(iv->us);
}

always_inline hot static inline void
data_read_interval(uint8_t** pos, Interval* iv)
{
	if (unlikely(**pos != AM_INTERVAL))
		data_error(*pos, AM_INTERVAL);
	*pos += data_size_type();
	int64_t value;
	data_read_integer(pos, &value);
	iv->m = value;
	data_read_integer(pos, &value);
	iv->d = value;
	data_read_integer(pos, &value);
	iv->us = value;
}

always_inline hot static inline void
data_write_interval(uint8_t** pos, Interval* iv)
{
	uint8_t* data = *pos;
	*data = AM_INTERVAL;
	*pos += data_size_type();
	data_write_integer(pos, iv->m);
	data_write_integer(pos, iv->d);
	data_write_integer(pos, iv->us);
}

always_inline hot static inline bool
data_is_interval(uint8_t* data)
{
	return *data >= AM_INTERVAL;
}

// timestamp

always_inline hot static inline int
data_size_timestamp(uint64_t value)
{
	return data_size_type() + data_size_integer(value);
}

always_inline hot static inline void
data_read_timestamp(uint8_t** pos, int64_t* value)
{
	if (unlikely(**pos != AM_TIMESTAMP))
		data_error(*pos, AM_TIMESTAMP);
	*pos += data_size_type();
	data_read_integer(pos, value);
}

always_inline hot static inline void
data_write_timestamp(uint8_t** pos, uint64_t value)
{
	uint8_t* data = *pos;
	*data = AM_TIMESTAMP;
	*pos += data_size_type();
	data_write_integer(pos, value);
}

always_inline hot static inline bool
data_is_timestamp(uint8_t* data)
{
	return *data == AM_TIMESTAMP;
}
