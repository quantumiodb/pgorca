/*
 * compat/catalog/pg_operator_oids.h
 *
 * Named OID constants for commonly-used PostgreSQL operators.
 * These are defined in Cloudberry's pg_operator_d.h but not in PG18.
 * OIDs are stable standard PostgreSQL operator OIDs.
 */
#ifndef COMPAT_PG_OPERATOR_OIDS_H
#define COMPAT_PG_OPERATOR_OIDS_H

#define Int4AddOperator             551
#define OIDTextConcatenateOperator  654
#define Int8AddOperator             684
#define DateIntervalAddOperator     1076
#define DateInt4AddOperator         1100
#define DateTimeAddOperator         1360
#define DateTimetzAddOperator       1361
#define NumericAddOperator          1724
#define TimestampIntervalAddOperator  2032
#define IntervalTimestampAddOperator  2551
/* Int4DateAddOperator same as DateInt4AddOperator - avoid duplicate case */
/* #define Int4DateAddOperator      1100 */

#endif /* COMPAT_PG_OPERATOR_OIDS_H */
