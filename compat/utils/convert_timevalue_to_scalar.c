/*
 * compat/utils/convert_timevalue_to_scalar.c
 *
 * Ported from Apache Cloudberry (src/backend/utils/adt/selfuncs.c).
 *
 * Converts a time-related datum to a double scalar value for use in
 * selectivity estimation.  Supports TIMESTAMP, TIMESTAMPTZ, DATE, INTERVAL,
 * TIME, and TIMETZ.
 *
 * Called from gpdbwrappers.cpp:
 *   gpdb::ConvertTimeValueToScalar() → convert_timevalue_to_scalar()
 */

#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "datatype/timestamp.h"
#include "utils/date.h"
#include "utils/timestamp.h"

#include "compat/utils/convert_timevalue_to_scalar.h"

/*
 * convert_timevalue_to_scalar
 *
 * Convert a time-related Datum to a scalar double for statistics purposes.
 * Sets *failure = true and returns 0 for unrecognised type OIDs.
 */
double
convert_timevalue_to_scalar(Datum value, Oid typid, bool *failure)
{
	switch (typid)
	{
		case TIMESTAMPOID:
			return DatumGetTimestamp(value);
		case TIMESTAMPTZOID:
			return DatumGetTimestampTz(value);
		case DATEOID:
			return date2timestamp_no_overflow(DatumGetDateADT(value));
		case INTERVALOID:
			{
				Interval   *interval = DatumGetIntervalP(value);

				/*
				 * Convert the month part of Interval to days using assumed
				 * average month length of 365.25/12.0 days.  Not too
				 * accurate, but plenty good enough for our purposes.
				 */
				return interval->time + interval->day * (double) USECS_PER_DAY +
					interval->month * ((DAYS_PER_YEAR / (double) MONTHS_PER_YEAR) * USECS_PER_DAY);
			}
		case TIMEOID:
			return DatumGetTimeADT(value);
		case TIMETZOID:
			{
				TimeTzADT  *timetz = DatumGetTimeTzADTP(value);

				/* use GMT-equivalent time */
				return (double) (timetz->time + (timetz->zone * 1000000.0));
			}
	}

	*failure = true;
	return 0;
}
