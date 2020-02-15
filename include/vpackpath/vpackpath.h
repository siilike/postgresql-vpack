/*-------------------------------------------------------------------------
 *
 * vpackpath.h
 *	Definitions for vpackpath datatype
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/include/utils/vpackpath.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VPACKPATH_H
#define VPACKPATH_H

#include "fmgr.h"
#include "utils/jsonb.h"
#include "nodes/pg_list.h"

typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		header;			/* version and flags (see below) */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} VpackPath;

#define VPACKPATH_VERSION	(0x01)
#define VPACKPATH_LAX		(0x80000000)
#define VPACKPATH_HDRSZ		(offsetof(VpackPath, data))

#define DatumGetVpackPathP(d)			((VpackPath *) DatumGetPointer(PG_DETOAST_DATUM(d)))
#define DatumGetVpackPathPCopy(d)		((VpackPath *) DatumGetPointer(PG_DETOAST_DATUM_COPY(d)))
#define PG_GETARG_VPACKPATH_P(x)			DatumGetVpackPathP(PG_GETARG_DATUM(x))
#define PG_GETARG_VPACKPATH_P_COPY(x)	DatumGetVpackPathPCopy(PG_GETARG_DATUM(x))
#define PG_RETURN_VPACKPATH_P(p)			PG_RETURN_POINTER(p)

#define jspIsScalar(type) ((type) >= jpiNull && (type) <= jpiBool)

/*
 * All node's type of vpackpath expression
 */
typedef enum VpackPathItemType
{
	jpiNull = jbvNull,			/* NULL literal */
	jpiString = jbvString,		/* string literal */
	jpiNumeric = jbvNumeric,	/* numeric literal */
	jpiBool = jbvBool,			/* boolean literal: TRUE or FALSE */
	jpiAnd,						/* predicate && predicate */
	jpiOr,						/* predicate || predicate */
	jpiNot,						/* ! predicate */
	jpiIsUnknown,				/* (predicate) IS UNKNOWN */
	jpiEqual,					/* expr == expr */
	jpiNotEqual,				/* expr != expr */
	jpiLess,					/* expr < expr */
	jpiGreater,					/* expr > expr */
	jpiLessOrEqual,				/* expr <= expr */
	jpiGreaterOrEqual,			/* expr >= expr */
	jpiAdd,						/* expr + expr */
	jpiSub,						/* expr - expr */
	jpiMul,						/* expr * expr */
	jpiDiv,						/* expr / expr */
	jpiMod,						/* expr % expr */
	jpiPlus,					/* + expr */
	jpiMinus,					/* - expr */
	jpiAnyArray,				/* [*] */
	jpiAnyKey,					/* .* */
	jpiIndexArray,				/* [subscript, ...] */
	jpiAny,						/* .** */
	jpiKey,						/* .key */
	jpiCurrent,					/* @ */
	jpiRoot,					/* $ */
	jpiVariable,				/* $variable */
	jpiFilter,					/* ? (predicate) */
	jpiExists,					/* EXISTS (expr) predicate */
	jpiType,					/* .type() item method */
	jpiSize,					/* .size() item method */
	jpiAbs,						/* .abs() item method */
	jpiFloor,					/* .floor() item method */
	jpiCeiling,					/* .ceiling() item method */
	jpiDouble,					/* .double() item method */
	jpiKeyValue,				/* .keyvalue() item method */
	jpiSubscript,				/* array subscript: 'expr' or 'expr TO expr' */
	jpiLast,					/* LAST array subscript */
	jpiStartsWith,				/* STARTS WITH predicate */
	jpiLikeRegex,				/* LIKE_REGEX predicate */
} VpackPathItemType;

/* XQuery regex mode flags for LIKE_REGEX predicate */
#define JSP_REGEX_ICASE		0x01	/* i flag, case insensitive */
#define JSP_REGEX_DOTALL	0x02	/* s flag, dot matches newline */
#define JSP_REGEX_MLINE		0x04	/* m flag, ^/$ match at newlines */
#define JSP_REGEX_WSPACE	0x08	/* x flag, ignore whitespace in pattern */
#define JSP_REGEX_QUOTE		0x10	/* q flag, no special characters */

/*
 * Support functions to parse/construct binary value.
 * Unlike many other representation of expression the first/main
 * node is not an operation but left operand of expression. That
 * allows to implement cheap follow-path descending in jsonb
 * structure and then execute operator with right operand
 */

typedef struct VpackPathItem
{
	VpackPathItemType type;

	/* position form base to next node */
	int32		nextPos;

	/*
	 * pointer into VpackPath value to current node, all positions of current
	 * are relative to this base
	 */
	char	   *base;

	union
	{
		/* classic operator with two operands: and, or etc */
		struct
		{
			int32		left;
			int32		right;
		}			args;

		/* any unary operation */
		int32		arg;

		/* storage for jpiIndexArray: indexes of array */
		struct
		{
			int32		nelems;
			struct
			{
				int32		from;
				int32		to;
			}		   *elems;
		}			array;

		/* jpiAny: levels */
		struct
		{
			uint32		first;
			uint32		last;
		}			anybounds;

		struct
		{
			char	   *data;	/* for bool, numeric and string/key */
			int32		datalen;	/* filled only for string/key */
		}			value;

		struct
		{
			int32		expr;
			char	   *pattern;
			int32		patternlen;
			uint32		flags;
		}			like_regex;
	}			content;
} VpackPathItem;

#define jspHasNext(jsp) ((jsp)->nextPos > 0)

extern void jspInit(VpackPathItem *v, VpackPath *js);
extern void jspInitByBuffer(VpackPathItem *v, char *base, int32 pos);
extern bool jspGetNext(VpackPathItem *v, VpackPathItem *a);
extern void jspGetArg(VpackPathItem *v, VpackPathItem *a);
extern void jspGetLeftArg(VpackPathItem *v, VpackPathItem *a);
extern void jspGetRightArg(VpackPathItem *v, VpackPathItem *a);
extern Numeric jspGetNumeric(VpackPathItem *v);
extern bool jspGetBool(VpackPathItem *v);
extern char *jspGetString(VpackPathItem *v, int32 *len);
extern bool jspGetArraySubscript(VpackPathItem *v, VpackPathItem *from,
								 VpackPathItem *to, int i);

extern const char *jspOperationName(VpackPathItemType type);

/*
 * Parsing support data structures.
 */

typedef struct VpackPathParseItem VpackPathParseItem;

struct VpackPathParseItem
{
	VpackPathItemType type;
	VpackPathParseItem *next;	/* next in path */

	union
	{

		/* classic operator with two operands: and, or etc */
		struct
		{
			VpackPathParseItem *left;
			VpackPathParseItem *right;
		}			args;

		/* any unary operation */
		VpackPathParseItem *arg;

		/* storage for jpiIndexArray: indexes of array */
		struct
		{
			int			nelems;
			struct
			{
				VpackPathParseItem *from;
				VpackPathParseItem *to;
			}		   *elems;
		}			array;

		/* jpiAny: levels */
		struct
		{
			uint32		first;
			uint32		last;
		}			anybounds;

		struct
		{
			VpackPathParseItem *expr;
			char	   *pattern;	/* could not be not null-terminated */
			uint32		patternlen;
			uint32		flags;
		}			like_regex;

		/* scalars */
		Numeric numeric;
		bool		boolean;
		struct
		{
			uint32		len;
			char	   *val;	/* could not be not null-terminated */
		}			string;
	}			value;
};

typedef struct VpackPathParseResult
{
	VpackPathParseItem *expr;
	bool		lax;
} VpackPathParseResult;

extern VpackPathParseResult *parsevpackpath(const char *str, int len);

extern int	jspConvertRegexFlags(uint32 xflags);

#endif
