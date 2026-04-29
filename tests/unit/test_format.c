
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

#include <amelie_test>

void
test_format(void* arg)
{
	unused(arg);

	char buf[128];
	int64_t value = -123;

	// 1
	char result1[] = "777hello world -123, -9223372036854775808, 18446744073709551615";
	format(buf, sizeof(buf), "{d}{s} {s} {i64}, {i64}, {u64}",
	       777,
	       "hello", "world",
	       value,
	       INT64_MIN,
	       UINT64_MAX); // 18446744073709551615ULL
	test(! strcmp(buf, result1));

	// 2
	char result2[] = "hello       world";
	format(buf, sizeof(buf), "{-12s} {s}", "hello", "world");
	test(! strcmp(buf, result2));

	// 3
	char result3[] = "hello world";
	Str str;
	str_set_cstr(&str, "hello");
	format(buf, sizeof(buf), "{str} {s}", &str, "world");
	test(! strcmp(buf, result3));

	// 4
	char result4[] = "hello world";
	Buf b;
	buf_init(&b);
	defer_buf(&b);
	buf_write(&b, "hello", 5);
	format(buf, sizeof(buf), "{buf} {s}", &b, "world");
	test(! strcmp(buf, result4));

	// 5
	str_set_cstr(&str, "hello");

	format(buf, 12, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello world"));

	format(buf, 11, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello worl"));

	format(buf, 10, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello wor"));

	format(buf, 9, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello wo"));

	format(buf, 8, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello w"));

	format(buf, 7, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello "));

	format(buf, 6, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hello"));

	format(buf, 5, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hell"));

	format(buf, 4, "{str} {s}", &str, "world");
	test(! strcmp(buf, "hel"));

	snprintf(buf, 4, "%s %s", "hello", "world");
	test(! strcmp(buf, "hel"));

	format(buf, 3, "{str} {s}", &str, "world");
	test(! strcmp(buf, "he"));

	format(buf, 2, "{str} {s}", &str, "world");
	test(! strcmp(buf, "h"));

	format(buf, 1, "{str} {s}", &str, "world");
	test(! strcmp(buf, ""));

	// 6
	format(buf, 11, "{d}", INT_MAX);
	test(! strcmp(buf, "2147483647"));

	format(buf, 10, "{d}", INT_MAX);
	test(! strcmp(buf, "214748364"));

	// 7
	char result7[] = "Content-Type: text/plain\r\n";
	format(buf, sizeof(buf), "Content-Type: {s}\r\n", "text/plain");
	test(! strcmp(buf, result7));

	// 8
	char result8[] = "{\"id\": 25}";
	format(buf, sizeof(buf), "{{\"{s}\": 25}}", "id");
	test(! strcmp(buf, result8));

	// 9
	char result9[] = "{\"id\": 25}";
	format(buf, sizeof(buf), "{{{qs}: 25}}", "id");
	test(! strcmp(buf, result9));
}
