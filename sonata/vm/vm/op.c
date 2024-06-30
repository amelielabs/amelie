
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_user.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_row.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>

OpDesc ops[] =
{
	// control
	{ CRET,               "ret"               },
	{ CNOP,               "nop"               },
	{ CJMP,               "jmp"               },
	{ CJTR,               "jtr"               },
	{ CJNTR,              "jntr"              },

	// result
	{ CSEND_HASH,         "send_hash"         },
	{ CSEND,              "send"              },
	{ CSEND_FIRST,        "send_first"        },
	{ CSEND_ALL,          "send_all"          },
	{ CRECV,              "recv"              },
	{ CRECV_TO,           "recv_to"           },
	{ CRESULT,            "result"            },
	{ CBODY,              "body"              },

	// cte
	{ CCTE_SET,           "cte_set"           },

	// misc
	{ CSLEEP,             "sleep"             },

	// stack
	{ CPUSH,              "push"              },
	{ CPOP,               "pop"               },

	// data
	{ CNULL,              "null"              },
	{ CBOOL,              "bool"              },
	{ CINT,               "int"               },
	{ CINT_MIN,           "int_min"           },
	{ CREAL,              "real"              },
	{ CSTRING,            "string"            },
	{ CSTRING_MIN,        "string_min"        },

	// arguments
	{ CARG,               "arg"               },

	// expr
	{ CBOR,               "bor"               },
	{ CBAND,              "band"              },
	{ CBXOR,              "bxor"              },
	{ CSHL,               "shl"               },
	{ CSHR,               "shr"               },

	{ CNOT,               "not"               },
	{ CEQU,               "equ"               },
	{ CNEQU,              "nequ"              },
	{ CGTE,               "gte"               },
	{ CGT,                "gt"                },
	{ CLTE,               "lte"               },
	{ CLT,                "lt"                },

	{ CADD,               "add"               },
	{ CSUB,               "sub"               },
	{ CMUL,               "mul"               },
	{ CDIV,               "div"               },
	{ CMOD,               "mod"               },
	{ CNEG,               "neg"               },
	{ CCAT,               "cat"               },
	{ CIDX,               "idx"               },

	// object
	{ CMAP,               "map"               },
	{ CARRAY,             "array"             },

	// column
	{ CASSIGN,            "assign"            },

	// set
	{ CSET,               "set"               },
	{ CSET_ORDERED,       "set_ordered"       },
	{ CSET_SORT,          "set_sort"          },
	{ CSET_ADD,           "set_add"           },

	// merge
	{ CMERGE,             "merge"             },
	{ CMERGE_RECV,        "merge_recv"        },

	// group
	{ CGROUP,             "group"             },
	{ CGROUP_ADD,         "group_add"         },
	{ CGROUP_WRITE,       "group_write"       },
	{ CGROUP_GET,         "group_get"         },
	{ CGROUP_READ,        "group_read"        },
	{ CGROUP_READ_AGGR,   "group_read_aggr"   },
	{ CGROUP_MERGE_RECV,  "group_merge_recv"  },

	// ref
	{ CREF,               "ref"               },
	{ CREF_KEY,           "ref_key"           },

	// counters
	{ CCNTR_INIT,         "cntr_init"         },
	{ CCNTR_GTE,          "cntr_gte"          },
	{ CCNTR_LTE,          "cntr_lte"          },

	// cursor
	{ CCURSOR_OPEN,       "cursor_open"       },
	{ CCURSOR_OPEN_EXPR,  "cursor_open_expr"  },
	{ CCURSOR_OPEN_CTE,   "cursor_open_cte"   },
	{ CCURSOR_PREPARE,    "cursor_prepare"    },
	{ CCURSOR_CLOSE,      "cursor_close"      },
	{ CCURSOR_NEXT,       "cursor_next"       },
	{ CCURSOR_READ,       "cursor_read"       },
	{ CCURSOR_IDX,        "cursor_idx"        },

	// functions
	{ CCALL,              "call"              },

	// dml
	{ CINSERT,            "insert"            },
	{ CUPDATE,            "update"            },
	{ CDELETE,            "delete"            },
	{ CUPSERT,            "upsert"            }
};

static inline void
op_write(Buf* output, Op* op, int pos, bool a, bool b, bool c, char *fmt, ...)
{
	buf_printf(output, "%2d  %18s   ", pos, ops[op->op].name);

	if (a)
		buf_printf(output, "%6" PRIi64 " ", op->a);
	else
		buf_printf(output, "%6s ", "-");

	if (b)
		buf_printf(output, "%6" PRIi64 " ", op->b);
	else
		buf_printf(output, "%6s ", "-");

	if (c)
		buf_printf(output, "%6" PRIi64, op->c);
	else
		buf_printf(output, "%6s", "-");

	if (fmt)
	{
		buf_printf(output, "      # ");
		va_list args;
		va_start(args, fmt);
		buf_vprintf(output, fmt, args);
		va_end(args);
	}
}

void
op_dump(Code* self, CodeData* data, Buf* output, Str* section)
{
	buf_printf(output, "\n");
	buf_printf(output, "bytecode [%.*s]\n", str_size(section), str_of(section));
	buf_printf(output, "--------\n");

	auto op  = (Op*)self->code.start;
	auto end = (Op*)self->code.position;

	int pos = 0;
	while (op < end)
	{
		switch (op->op) {
		case CSTRING:
		{
			Str str;
			code_data_at_string(data, op->b, &str);
			op_write(output, op, pos, true, true, true, "%.*s",
			         str_size(&str), str_of(&str));
			break;
		}
		case CREAL:
		{
			double real = code_data_at_real(data, op->b);
			op_write(output, op, pos, true, true, true, "%g", real);
			break;
		}
		case CSEND_HASH:
		case CSEND:
		{
			auto table = (Table*)op->c;
			op_write(output, op, pos, true, true, false,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CINSERT:
		{
			auto table = (Table*)op->a;
			op_write(output, op, pos, false, true, true,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CCURSOR_OPEN:
		{
			Str schema;
			Str table;
			Str index;
			uint8_t* ref = code_data_at(data, op->b);
			data_read_string(&ref, &schema);
			data_read_string(&ref, &table);
			data_read_string(&ref, &index);
			op_write(output, op, pos, true, true, true,
			         "%.*s.%.*s (%.*s)%s",
			         str_size(&schema),
			         str_of(&schema),
			         str_size(&table),
			         str_of(&table),
			         str_size(&index),
			         str_of(&index),
			         op->d ? ", lookup": "");
			break;
		}
		case CCURSOR_PREPARE:
		{
			auto table = (Table*)op->b;
			op_write(output, op, pos, true, false, true,
			         "%.*s.%.*s",
			         str_size(&table->config->schema),
			         str_of(&table->config->schema),
			         str_size(&table->config->name),
			         str_of(&table->config->name));
			break;
		}
		case CCURSOR_IDX:
		{
			if (op->d == -1) {
				op_write(output, op, pos, true, true, true, NULL);
			} else
			{
				Str name;
				code_data_at_string(data, op->d, &name);
				op_write(output, op, pos, true, true, true,
				         "%.*s", str_size(&name), str_of(&name));
			}
			break;
		}
		case CCALL:
		{
			auto function = (Function*)op->b;
			op_write(output, op, pos, true, false, true,
			         "%.*s.%.*s()",
			         str_size(&function->schema),
			         str_of(&function->schema),
			         str_size(&function->name),
			         str_of(&function->name));
			break;
		}
		default:
			op_write(output, op, pos, true, true, true, NULL);
			break;
		}
		buf_printf(output, "\n");

		op++;
		pos++;
	}

	buf_printf(output, "\n");
}
