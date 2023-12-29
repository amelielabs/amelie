#pragma once

//
// monotone
//
// SQL OLTP database
//

#define HISTOGRAM_COUNT 155

typedef struct Histogram Histogram;

struct Histogram
{
	double min, max, sum;
	double sumsq;
	int    power;
	size_t buckets[HISTOGRAM_COUNT];
	size_t size;
};

static const double
histogram_buckets[HISTOGRAM_COUNT] =
{
	1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18,	20,  25,  30,  35,	40,  45,
	50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350,  400,	450,
	500, 600, 700, 800, 900, 1000, 1200, 1400, 1600,  1800,  2000,	2500,  3000,
	3500, 4000, 4500,  5000,  6000,  7000,	8000,  9000,  10000,  12000,  14000,
	16000, 18000, 20000,  25000,  30000,  35000,  40000,  45000,  50000,  60000,
	70000,	80000,	90000,	100000,  120000,  140000,  160000,	180000,  200000,
	250000, 300000, 350000, 400000,  450000,  500000,  600000,	700000,  800000,
	900000, 1000000,  1200000,	1400000,  1600000,	1800000,  2000000,	2500000,
	3000000, 3500000, 4000000,	4500000,  5000000,	6000000,  7000000,	8000000,
	9000000,  10000000,  12000000,	14000000,  16000000,   18000000,   20000000,
	25000000,  30000000,  35000000,  40000000,	45000000,  50000000,   60000000,
	70000000, 80000000, 90000000, 100000000,  120000000,  140000000,  160000000,
	180000000,	 200000000,   250000000,   300000000,	350000000,	  400000000,
	450000000,	 500000000,   600000000,   700000000,	800000000,	  900000000,
	1000000000,  1200000000,  1400000000,  1600000000,	1800000000,  2000000000,
	2500000000.0,  3000000000.0,   3500000000.0,   4000000000.0,   4500000000.0,
	5000000000.0,  6000000000.0,   7000000000.0,   8000000000.0,   9000000000.0,
    1e200, INFINITY
};

static inline void
histogram_init(Histogram* self)
{
	memset(self, 0, sizeof(*self));
	self->min   = INFINITY;
	self->power = 3;
}

static inline void
histogram_add(Histogram* self, double val)
{
	val *= pow(10, self->power);
	size_t begin = 0;
	size_t end = HISTOGRAM_COUNT - 1;
	size_t mid;
	while (1) {
		mid = begin / 2 + end / 2;
		if (mid == begin) {
			for (mid = end; mid > begin; mid--) {
				if (histogram_buckets[mid - 1] < val)
					break;
			}
			break;
		}
		if (histogram_buckets[mid - 1] < val)
			begin = mid;
		else
			end = mid;
	}
	if (self->min > val)
		self->min = val;
	if (self->max < val)
		self->max = val;
	self->sum += val;
	self->sumsq += val * val;
	self->buckets[mid]++;
	self->size++;
}

static inline void
histogram_print(Histogram* self)
{
	if (self->size == 0)
		return;

	printf("\n");
	printf("[%7s, %7s]\t%11s\t%7s\n", "t min", "t max",
	       "ops count", "%");
	printf("--------------------------------------------------\n");

	int i = 0;
	for (; i < HISTOGRAM_COUNT; i++) {
		if (self->buckets[i] == 0.0)
			continue;
		double percents = (double) self->buckets[i] / self->size;
		printf("[%7.0lf, %7.0lf]\t%11zu\t%7.2lf ",
		       (i > 0) ? histogram_buckets[i - 1] : 0.0,
		       histogram_buckets[i],
		       self->buckets[i], percents * 1e2);
		putchar('\n');
	}
	double avg_latency = self->sum / self->size;
	printf("--------------------------------------------------\n");
	printf("total:%5s%7.0lf\t%11zu\t   100%%\n", "", self->sum, self->size);
	printf("\n");
	printf("min latency       : %7.6lf * 1e%d sec/op\n", self->min, -self->power);
	printf("avg latency       : %7.6lf * 1e%d sec/op\n", avg_latency, -self->power);
	printf("max latency       : %7.6lf * 1e%d sec/op\n", self->max, -self->power);
	printf("avg throughput    : %7.0lf ops/sec\n",
	       (double)self->size / (self->sum * pow(10, -self->power)));
}

static inline double
histogram_time(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec + 1e-6 * tv.tv_usec;
}
