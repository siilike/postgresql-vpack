%{
/*-------------------------------------------------------------------------
 *
 * vpackpath_gram.y
 *	 Grammar definitions for vpackpath datatype
 *
 * Transforms tokenized vpackpath into tree of VpackPathParseItem structs.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/vpackpath_gram.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "vpackpath/vpackpath.h"

/* struct VpackPathString is shared between scan and gram */
typedef struct VpackPathString
{
	char	   *val;
	int			len;
	int			total;
}			VpackPathString;

union YYSTYPE;

/* flex 2.5.4 doesn't bother with a decl for this */
int	vpackpath_yylex(union YYSTYPE *yylval_param);
int	vpackpath_yyparse(VpackPathParseResult **result);
void vpackpath_yyerror(VpackPathParseResult **result, const char *message);

static VpackPathParseItem *makeItemType(VpackPathItemType type);
static VpackPathParseItem *makeItemString(VpackPathString *s);
static VpackPathParseItem *makeItemVariable(VpackPathString *s);
static VpackPathParseItem *makeItemKey(VpackPathString *s);
static VpackPathParseItem *makeItemNumeric(VpackPathString *s);
static VpackPathParseItem *makeItemBool(bool val);
static VpackPathParseItem *makeItemBinary(VpackPathItemType type,
										 VpackPathParseItem *la,
										 VpackPathParseItem *ra);
static VpackPathParseItem *makeItemUnary(VpackPathItemType type,
										VpackPathParseItem *a);
static VpackPathParseItem *makeItemList(List *list);
static VpackPathParseItem *makeIndexArray(List *list);
static VpackPathParseItem *makeAny(int first, int last);
static VpackPathParseItem *makeItemLikeRegex(VpackPathParseItem *expr,
											VpackPathString *pattern,
											VpackPathString *flags);

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

%}

/* BISON Declarations */
%pure-parser
%expect 0
%name-prefix="vpackpath_yy"
%error-verbose
%parse-param {VpackPathParseResult **result}

%union {
	VpackPathString		str;
	List			   *elems;	/* list of VpackPathParseItem */
	List			   *indexs;	/* list of integers */
	VpackPathParseItem  *value;
	VpackPathParseResult *result;
	VpackPathItemType	optype;
	bool				boolean;
	int					integer;
}

%token	<str>		TO_P NULL_P TRUE_P FALSE_P IS_P UNKNOWN_P EXISTS_P
%token	<str>		IDENT_P STRING_P NUMERIC_P INT_P VARIABLE_P
%token	<str>		OR_P AND_P NOT_P
%token	<str>		LESS_P LESSEQUAL_P EQUAL_P NOTEQUAL_P GREATEREQUAL_P GREATER_P
%token	<str>		ANY_P STRICT_P LAX_P LAST_P STARTS_P WITH_P LIKE_REGEX_P FLAG_P
%token	<str>		ABS_P SIZE_P TYPE_P FLOOR_P DOUBLE_P CEILING_P KEYVALUE_P

%type	<result>	result

%type	<value>		scalar_value path_primary expr array_accessor
					any_path accessor_op key predicate delimited_predicate
					index_elem starts_with_initial expr_or_predicate

%type	<elems>		accessor_expr

%type	<indexs>	index_list

%type	<optype>	comp_op method

%type	<boolean>	mode

%type	<str>		key_name

%type	<integer>	any_level

%left	OR_P
%left	AND_P
%right	NOT_P
%left	'+' '-'
%left	'*' '/' '%'
%left	UMINUS
%nonassoc '(' ')'

/* Grammar follows */
%%

result:
	mode expr_or_predicate			{
										*result = palloc(sizeof(VpackPathParseResult));
										(*result)->expr = $2;
										(*result)->lax = $1;
									}
	| /* EMPTY */					{ *result = NULL; }
	;

expr_or_predicate:
	expr							{ $$ = $1; }
	| predicate						{ $$ = $1; }
	;

mode:
	STRICT_P						{ $$ = false; }
	| LAX_P							{ $$ = true; }
	| /* EMPTY */					{ $$ = true; }
	;

scalar_value:
	STRING_P						{ $$ = makeItemString(&$1); }
	| NULL_P						{ $$ = makeItemString(NULL); }
	| TRUE_P						{ $$ = makeItemBool(true); }
	| FALSE_P						{ $$ = makeItemBool(false); }
	| NUMERIC_P						{ $$ = makeItemNumeric(&$1); }
	| INT_P							{ $$ = makeItemNumeric(&$1); }
	| VARIABLE_P 					{ $$ = makeItemVariable(&$1); }
	;

comp_op:
	EQUAL_P							{ $$ = jpiEqual; }
	| NOTEQUAL_P					{ $$ = jpiNotEqual; }
	| LESS_P						{ $$ = jpiLess; }
	| GREATER_P						{ $$ = jpiGreater; }
	| LESSEQUAL_P					{ $$ = jpiLessOrEqual; }
	| GREATEREQUAL_P				{ $$ = jpiGreaterOrEqual; }
	;

delimited_predicate:
	'(' predicate ')'				{ $$ = $2; }
	| EXISTS_P '(' expr ')'			{ $$ = makeItemUnary(jpiExists, $3); }
	;

predicate:
	delimited_predicate				{ $$ = $1; }
	| expr comp_op expr				{ $$ = makeItemBinary($2, $1, $3); }
	| predicate AND_P predicate		{ $$ = makeItemBinary(jpiAnd, $1, $3); }
	| predicate OR_P predicate		{ $$ = makeItemBinary(jpiOr, $1, $3); }
	| NOT_P delimited_predicate 	{ $$ = makeItemUnary(jpiNot, $2); }
	| '(' predicate ')' IS_P UNKNOWN_P
									{ $$ = makeItemUnary(jpiIsUnknown, $2); }
	| expr STARTS_P WITH_P starts_with_initial
									{ $$ = makeItemBinary(jpiStartsWith, $1, $4); }
	| expr LIKE_REGEX_P STRING_P 	{ $$ = makeItemLikeRegex($1, &$3, NULL); }
	| expr LIKE_REGEX_P STRING_P FLAG_P STRING_P
									{ $$ = makeItemLikeRegex($1, &$3, &$5); }
	;

starts_with_initial:
	STRING_P						{ $$ = makeItemString(&$1); }
	| VARIABLE_P					{ $$ = makeItemVariable(&$1); }
	;

path_primary:
	scalar_value					{ $$ = $1; }
	| '$'							{ $$ = makeItemType(jpiRoot); }
	| '@'							{ $$ = makeItemType(jpiCurrent); }
	| LAST_P						{ $$ = makeItemType(jpiLast); }
	;

accessor_expr:
	path_primary					{ $$ = list_make1($1); }
	| '(' expr ')' accessor_op		{ $$ = list_make2($2, $4); }
	| '(' predicate ')' accessor_op	{ $$ = list_make2($2, $4); }
	| accessor_expr accessor_op		{ $$ = lappend($1, $2); }
	;

expr:
	accessor_expr					{ $$ = makeItemList($1); }
	| '(' expr ')'					{ $$ = $2; }
	| '+' expr %prec UMINUS			{ $$ = makeItemUnary(jpiPlus, $2); }
	| '-' expr %prec UMINUS			{ $$ = makeItemUnary(jpiMinus, $2); }
	| expr '+' expr					{ $$ = makeItemBinary(jpiAdd, $1, $3); }
	| expr '-' expr					{ $$ = makeItemBinary(jpiSub, $1, $3); }
	| expr '*' expr					{ $$ = makeItemBinary(jpiMul, $1, $3); }
	| expr '/' expr					{ $$ = makeItemBinary(jpiDiv, $1, $3); }
	| expr '%' expr					{ $$ = makeItemBinary(jpiMod, $1, $3); }
	;

index_elem:
	expr							{ $$ = makeItemBinary(jpiSubscript, $1, NULL); }
	| expr TO_P expr				{ $$ = makeItemBinary(jpiSubscript, $1, $3); }
	;

index_list:
	index_elem						{ $$ = list_make1($1); }
	| index_list ',' index_elem		{ $$ = lappend($1, $3); }
	;

array_accessor:
	'[' '*' ']'						{ $$ = makeItemType(jpiAnyArray); }
	| '[' index_list ']'			{ $$ = makeIndexArray($2); }
	;

any_level:
	INT_P							{ $$ = pg_atoi($1.val, 4, 0); }
	| LAST_P						{ $$ = -1; }
	;

any_path:
	ANY_P							{ $$ = makeAny(0, -1); }
	| ANY_P '{' any_level '}'		{ $$ = makeAny($3, $3); }
	| ANY_P '{' any_level TO_P any_level '}'
									{ $$ = makeAny($3, $5); }
	;

accessor_op:
	'.' key							{ $$ = $2; }
	| '.' '*'						{ $$ = makeItemType(jpiAnyKey); }
	| array_accessor				{ $$ = $1; }
	| '.' any_path					{ $$ = $2; }
	| '.' method '(' ')'			{ $$ = makeItemType($2); }
	| '?' '(' predicate ')'			{ $$ = makeItemUnary(jpiFilter, $3); }
	;

key:
	key_name						{ $$ = makeItemKey(&$1); }
	;

key_name:
	IDENT_P
	| STRING_P
	| TO_P
	| NULL_P
	| TRUE_P
	| FALSE_P
	| IS_P
	| UNKNOWN_P
	| EXISTS_P
	| STRICT_P
	| LAX_P
	| ABS_P
	| SIZE_P
	| TYPE_P
	| FLOOR_P
	| DOUBLE_P
	| CEILING_P
	| KEYVALUE_P
	| LAST_P
	| STARTS_P
	| WITH_P
	| LIKE_REGEX_P
	| FLAG_P
	;

method:
	ABS_P							{ $$ = jpiAbs; }
	| SIZE_P						{ $$ = jpiSize; }
	| TYPE_P						{ $$ = jpiType; }
	| FLOOR_P						{ $$ = jpiFloor; }
	| DOUBLE_P						{ $$ = jpiDouble; }
	| CEILING_P						{ $$ = jpiCeiling; }
	| KEYVALUE_P					{ $$ = jpiKeyValue; }
	;
%%

/*
 * The helper functions below allocate and fill VpackPathParseItem's of various
 * types.
 */

static VpackPathParseItem *
makeItemType(VpackPathItemType type)
{
	VpackPathParseItem  *v = palloc(sizeof(*v));

	CHECK_FOR_INTERRUPTS();

	v->type = type;
	v->next = NULL;

	return v;
}

static VpackPathParseItem *
makeItemString(VpackPathString *s)
{
	VpackPathParseItem  *v;

	if (s == NULL)
	{
		v = makeItemType(jpiNull);
	}
	else
	{
		v = makeItemType(jpiString);
		v->value.string.val = s->val;
		v->value.string.len = s->len;
	}

	return v;
}

static VpackPathParseItem *
makeItemVariable(VpackPathString *s)
{
	VpackPathParseItem  *v;

	v = makeItemType(jpiVariable);
	v->value.string.val = s->val;
	v->value.string.len = s->len;

	return v;
}

static VpackPathParseItem *
makeItemKey(VpackPathString *s)
{
	VpackPathParseItem  *v;

	v = makeItemString(s);
	v->type = jpiKey;

	return v;
}

static VpackPathParseItem *
makeItemNumeric(VpackPathString *s)
{
	VpackPathParseItem  *v;

	v = makeItemType(jpiNumeric);
	v->value.numeric =
		DatumGetNumeric(DirectFunctionCall3(numeric_in,
											CStringGetDatum(s->val),
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(-1)));

	return v;
}

static VpackPathParseItem *
makeItemBool(bool val)
{
	VpackPathParseItem  *v = makeItemType(jpiBool);

	v->value.boolean = val;

	return v;
}

static VpackPathParseItem *
makeItemBinary(VpackPathItemType type, VpackPathParseItem *la, VpackPathParseItem *ra)
{
	VpackPathParseItem  *v = makeItemType(type);

	v->value.args.left = la;
	v->value.args.right = ra;

	return v;
}

static VpackPathParseItem *
makeItemUnary(VpackPathItemType type, VpackPathParseItem *a)
{
	VpackPathParseItem  *v;

	if (type == jpiPlus && a->type == jpiNumeric && !a->next)
		return a;

	if (type == jpiMinus && a->type == jpiNumeric && !a->next)
	{
		v = makeItemType(jpiNumeric);
		v->value.numeric =
			DatumGetNumeric(DirectFunctionCall1(numeric_uminus,
												NumericGetDatum(a->value.numeric)));
		return v;
	}

	v = makeItemType(type);

	v->value.arg = a;

	return v;
}

static VpackPathParseItem *
makeItemList(List *list)
{
	VpackPathParseItem  *head,
					   *end;
	ListCell		   *cell = list_head(list);

	head = end = (VpackPathParseItem *) lfirst(cell);

	if (!lnext(cell))
		return head;

	/* append items to the end of already existing list */
	while (end->next)
		end = end->next;

	for_each_cell(cell, lnext(cell))
	{
		VpackPathParseItem *c = (VpackPathParseItem *) lfirst(cell);

		end->next = c;
		end = c;
	}

	return head;
}

static VpackPathParseItem *
makeIndexArray(List *list)
{
	VpackPathParseItem  *v = makeItemType(jpiIndexArray);
	ListCell		   *cell;
	int					i = 0;

	Assert(list_length(list) > 0);
	v->value.array.nelems = list_length(list);

	v->value.array.elems = palloc(sizeof(v->value.array.elems[0]) *
								  v->value.array.nelems);

	foreach(cell, list)
	{
		VpackPathParseItem  *jpi = lfirst(cell);

		Assert(jpi->type == jpiSubscript);

		v->value.array.elems[i].from = jpi->value.args.left;
		v->value.array.elems[i++].to = jpi->value.args.right;
	}

	return v;
}

static VpackPathParseItem *
makeAny(int first, int last)
{
	VpackPathParseItem  *v = makeItemType(jpiAny);

	v->value.anybounds.first = (first >= 0) ? first : PG_UINT32_MAX;
	v->value.anybounds.last = (last >= 0) ? last : PG_UINT32_MAX;

	return v;
}

static VpackPathParseItem *
makeItemLikeRegex(VpackPathParseItem *expr, VpackPathString *pattern,
				  VpackPathString *flags)
{
	VpackPathParseItem  *v = makeItemType(jpiLikeRegex);
	int					i;
	int					cflags;

	v->value.like_regex.expr = expr;
	v->value.like_regex.pattern = pattern->val;
	v->value.like_regex.patternlen = pattern->len;

	/* Parse the flags string, convert to bitmask.  Duplicate flags are OK. */
	v->value.like_regex.flags = 0;
	for (i = 0; flags && i < flags->len; i++)
	{
		switch (flags->val[i])
		{
			case 'i':
				v->value.like_regex.flags |= JSP_REGEX_ICASE;
				break;
			case 's':
				v->value.like_regex.flags |= JSP_REGEX_DOTALL;
				break;
			case 'm':
				v->value.like_regex.flags |= JSP_REGEX_MLINE;
				break;
			case 'x':
				v->value.like_regex.flags |= JSP_REGEX_WSPACE;
				break;
			case 'q':
				v->value.like_regex.flags |= JSP_REGEX_QUOTE;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input syntax for type %s", "vpackpath"),
						 errdetail("unrecognized flag character \"%c\" in LIKE_REGEX predicate",
								   flags->val[i])));
				break;
		}
	}
	
	/* Convert flags to what RE_compile_and_cache needs */
	cflags = jspConvertRegexFlags(v->value.like_regex.flags);

	/* check regex validity */
	(void) RE_compile_and_cache(cstring_to_text_with_len(pattern->val,
														 pattern->len),
								cflags, DEFAULT_COLLATION_OID);

	return v;
}

/*
 * Convert from XQuery regex flags to those recognized by our regex library.
 */
int
jspConvertRegexFlags(uint32 xflags)
{
	/* By default, XQuery is very nearly the same as Spencer's AREs */
	int			cflags = REG_ADVANCED;

	/* Ignore-case means the same thing, too, modulo locale issues */
	if (xflags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;

	/* Per XQuery spec, if 'q' is specified then 'm', 's', 'x' are ignored */
	if (xflags & JSP_REGEX_QUOTE)
	{
		cflags &= ~REG_ADVANCED;
		cflags |= REG_QUOTE;
	}
	else
	{
		/* Note that dotall mode is the default in POSIX */
		if (!(xflags & JSP_REGEX_DOTALL))
			cflags |= REG_NLSTOP;
		if (xflags & JSP_REGEX_MLINE)
			cflags |= REG_NLANCH;

		/*
		 * XQuery's 'x' mode is related to Spencer's expanded mode, but it's
		 * not really enough alike to justify treating JSP_REGEX_WSPACE as
		 * REG_EXPANDED.  For now we treat 'x' as unimplemented; perhaps in
		 * future we'll modify the regex library to have an option for
		 * XQuery-style ignore-whitespace mode.
		 */
		if (xflags & JSP_REGEX_WSPACE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("XQuery \"x\" flag (expanded regular expressions) is not implemented")));
	}

	return cflags;
}

/*
 * vpackpath_scan.l is compiled as part of vpackpath_gram.y.  Currently, this is
 * unavoidable because vpackpath_gram does not create a .h file to export its
 * token symbols.  If these files ever grow large enough to be worth compiling
 * separately, that could be fixed; but for now it seems like useless
 * complication.
 */

#include "vpackpath_scan.c"
