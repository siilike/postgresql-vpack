/*-------------------------------------------------------------------------
 *
 * vpackpath_exec.c
 *	 Routines for SQL/JSON path execution.
 *
 * Vpackpath is executed in the global context stored in VpackPathExecContext,
 * which is passed to almost every function involved into execution.  Entry
 * point for vpackpath execution is executeVpackPath() function, which
 * initializes execution context including initial VpackPathItem and VpackSlice,
 * flags, stack for calculation of @ in filters.
 *
 * The result of vpackpath query execution is enum VpackPathExecResult and
 * if succeeded sequence of VpackSlice, written to VpackValueList *found, which
 * is passed through the vpackpath items.  When found == NULL, we're inside
 * exists-query and we're interested only in whether result is empty.  In this
 * case execution is stopped once first result item is found, and the only
 * execution result is VpackPathExecResult.  The values of VpackPathExecResult
 * are following:
 * - jperOk			-- result sequence is not empty
 * - jperNotFound	-- result sequence is empty
 * - jperError		-- error occurred during execution
 *
 * Vpackpath is executed recursively (see executeItem()) starting form the
 * first path item (which in turn might be, for instance, an arithmetic
 * expression evaluated separately).  On each step single VpackSlice obtained
 * from previous path item is processed.  The result of processing is a
 * sequence of VpackSlice (probably empty), which is passed to the next path
 * item one by one.  When there is no next path item, then VpackSlice is added
 * to the 'found' list.  When found == NULL, then execution functions just
 * return jperOk (see executeNextItem()).
 *
 * Many of vpackpath operations require automatic unwrapping of arrays in lax
 * mode.  So, if input value is array, then corresponding operation is
 * processed not on array itself, but on all of its members one by one.
 * executeItemOptUnwrapTarget() function have 'unwrap' argument, which indicates
 * whether unwrapping of array is needed.  When unwrap == true, each of array
 * members is passed to executeItemOptUnwrapTarget() again but with unwrap == false
 * in order to evade subsequent array unwrapping.
 *
 * All boolean expressions (predicates) are evaluated by executeBoolItem()
 * function, which returns tri-state VpackPathBool.  When error is occurred
 * during predicate execution, it returns jpbUnknown.  According to standard
 * predicates can be only inside filters.  But we support their usage as
 * vpackpath expression.  This helps us to implement @@ operator.  In this case
 * resulting VpackPathBool is transformed into Vpack bool or null.
 *
 * Arithmetic and boolean expression are evaluated recursively from expression
 * tree top down to the leaves.  Therefore, for binary arithmetic expressions
 * we calculate operands first.  Then we check that results are numeric
 * singleton lists, calculate the result and pass it to the next path item.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/vpackpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

extern "C" {

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/formatting.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "vpackpath/vpackpath.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/numeric.h"

} // extern "C"

#include "vpack.h"

extern "C" {

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct VpackBaseObjectInfo
{
	VpackSlice *jbc;
	int			id;
} JsonBaseObjectInfo;

/*
 * Context of vpackpath execution.
 */
typedef struct VpackPathExecContext
{
	Vpack	   *vars;			/* variables to substitute into vpackpath */
	VpackSlice *root;			/* for $ evaluation */
	VpackSlice *current;		/* for @ evaluation */
	VpackBaseObjectInfo baseObject;	/* "base object" for .keyvalue()
									 * evaluation */
	int			lastGeneratedObjectId;	/* "id" counter for .keyvalue()
										 * evaluation */
	int			innermostArraySize; /* for LAST array index evaluation */
	bool		laxMode;		/* true for "lax" mode, false for "strict"
								 * mode */
	bool		ignoreStructuralErrors; /* with "true" structural errors such
										 * as absence of required json item or
										 * unexpected json item type are
										 * ignored */
	bool		throwErrors;	/* with "false" all suppressible errors are
								 * suppressed */
} VpackPathExecContext;

/* Context for LIKE_REGEX execution. */
typedef struct VpackLikeRegexContext
{
	text	   *regex;
	int			cflags;
} JsonLikeRegexContext;

/* Result of vpackpath predicate evaluation */
typedef enum VpackPathBool
{
	jpbFalse = 0,
	jpbTrue = 1,
	jpbUnknown = 2
} VpackPathBool;

/* Result of vpackpath expression evaluation */
typedef enum VpackPathExecResult
{
	jperOk = 0,
	jperNotFound = 1,
	jperError = 2
} VpackPathExecResult;

#define jperIsError(jper)			((jper) == jperError)

/*
 * List of jsonb values with shortcut for single-value list.
 */
typedef struct VpackValueList
{
	VpackSlice *singleton;
	List	   *list;
} VpackValueList;

typedef struct VpackValueListIterator
{
	VpackSlice *value;
	ListCell   *next;
} VpackValueListIterator;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenseOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)
#define jspThrowErrors(cxt)				((cxt)->throwErrors)

/* Convenience macro: return or throw error depending on context */
#define RETURN_ERROR(throw_error) \
do { \
	if (jspThrowErrors(cxt)) \
		throw_error; \
	else \
		return jperError; \
} while (0)

typedef VpackPathBool (*VpackPathPredicateCallback) (VpackPathItem *jsp,
												   VpackSlice *larg,
												   VpackSlice *rarg,
												   void *param);
typedef Numeric (*BinaryArithmFunc) (Numeric num1, Numeric num2, bool *error);

static VpackPathExecResult executeVpackPath(VpackPath *path, Vpack *vars,
										  Vpack *vpack, bool throwErrors, VpackValueList *result);
static VpackPathExecResult executeItem(VpackPathExecContext *cxt,
									  VpackPathItem *jsp, VpackSlice *jb, VpackValueList *found);
static VpackPathExecResult executeItemOptUnwrapTarget(VpackPathExecContext *cxt,
													 VpackPathItem *jsp, VpackSlice *jb,
													 VpackValueList *found, bool unwrap);
static VpackPathExecResult executeItemUnwrapTargetArray(VpackPathExecContext *cxt,
													   VpackPathItem *jsp, VpackSlice *jb,
													   VpackValueList *found, bool unwrapElements);
static VpackPathExecResult executeNextItem(VpackPathExecContext *cxt,
										  VpackPathItem *cur, VpackPathItem *next,
										  VpackSlice *v, VpackValueList *found, bool copy);
static VpackPathExecResult executeItemOptUnwrapResult(VpackPathExecContext *cxt, VpackPathItem *jsp, VpackSlice *jb,
													 bool unwrap, VpackValueList *found);
static VpackPathExecResult executeItemOptUnwrapResultNoThrow(VpackPathExecContext *cxt, VpackPathItem *jsp,
															VpackSlice *jb, bool unwrap, VpackValueList *found);
static VpackPathBool executeBoolItem(VpackPathExecContext *cxt,
									VpackPathItem *jsp, VpackSlice *jb, bool canHaveNext);
static VpackPathBool executeNestedBoolItem(VpackPathExecContext *cxt,
										  VpackPathItem *jsp, VpackSlice *jb);
static VpackPathExecResult executeAnyItem(VpackPathExecContext *cxt,
										 VpackPathItem *jsp, VpackSlice *jbc, VpackValueList *found,
										 uint32 level, uint32 first, uint32 last,
										 bool ignoreStructuralErrors, bool unwrapNext);
static VpackPathBool executePredicate(VpackPathExecContext *cxt,
									 VpackPathItem *pred, VpackPathItem *larg, VpackPathItem *rarg,
									 VpackSlice *jb, bool unwrapRightArg,
									 VpackPathPredicateCallback exec, void *param);
static VpackPathExecResult executeBinaryArithmExpr(VpackPathExecContext *cxt,
												  VpackPathItem *jsp, VpackSlice *jb,
												  BinaryArithmFunc func, VpackValueList *found);
static VpackPathExecResult executeUnaryArithmExpr(VpackPathExecContext *cxt,
												 VpackPathItem *jsp, VpackSlice *jb, PGFunction func,
												 VpackValueList *found);
static VpackPathBool executeStartsWith(VpackPathItem *jsp,
									  VpackSlice *whole, VpackSlice *initial, void *param);
static VpackPathBool executeLikeRegex(VpackPathItem *jsp, VpackSlice *str,
									 VpackSlice *rarg, void *param);
static VpackPathExecResult executeNumericItemMethod(VpackPathExecContext *cxt,
												   VpackPathItem *jsp, VpackSlice *jb, bool unwrap, PGFunction func,
												   VpackValueList *found);
static VpackPathExecResult executeKeyValueMethod(VpackPathExecContext *cxt,
												VpackPathItem *jsp, VpackSlice *jb, VpackValueList *found);
static VpackPathExecResult appendBoolResult(VpackPathExecContext *cxt,
										   VpackPathItem *jsp, VpackValueList *found, VpackPathBool res);
static void getVpackPathItem(VpackPathExecContext *cxt, VpackPathItem *item,
							VpackBuffer<uint8_t> *value);
static void getVpackPathVariable(VpackPathExecContext *cxt,
								VpackPathItem *variable, Vpack *vars, VpackBuffer<uint8_t> *value);
static int	JsonbArraySize(VpackSlice *jb);
static VpackPathBool executeComparison(VpackPathItem *cmp, VpackSlice *lv,
									  VpackSlice *rv, void *p);
static VpackPathBool compareItems(int32 op, VpackSlice *jb1, VpackSlice *jb2);
static int	compareNumeric(Numeric a, Numeric b);
static VpackSlice *copyVpackSlice(VpackSlice *src);
static VpackSlice *intSlice(int nr);
static VpackSlice *intSlice0(int nr, VpackBuffer<uint8_t> *buffer);
static VpackPathExecResult getArrayIndex(VpackPathExecContext *cxt,
										VpackPathItem *jsp, VpackSlice *jb, int32 *index);
static VpackBaseObjectInfo setBaseObject(VpackPathExecContext *cxt,
										VpackSlice *jbv, int32 id);
static void VpackValueListAppend(VpackValueList *jvl, VpackSlice *jbv);
static int	VpackValueListLength(const VpackValueList *jvl);
static bool VpackValueListIsEmpty(VpackValueList *jvl);
static VpackSlice *VpackValueListHead(VpackValueList *jvl);
static List *VpackValueListGetList(VpackValueList *jvl);
static void VpackValueListInitIterator(const VpackValueList *jvl,
									  VpackValueListIterator *it);
static VpackSlice *VpackValueListNext(const VpackValueList *jvl,
									 VpackValueListIterator *it);
static VpackSlice *getScalar(VpackSlice *scalar, enum jbvType type);
static VpackSlice *wrapItemsInArray(const VpackValueList *items);

static Numeric VpackToNumeric(VpackSlice &s);
static VpackSlice* NumericToVpack(Numeric);

/****************** User interface to VpackPath executor ********************/

/*
 * vpack_path_exists
 *		Returns true if vpackpath returns at least one item for the specified
 *		Vpack value.  This function and vpack_path_match() are used to
 *		implement @? and @@ operators, which in turn are intended to have an
 *		index support.  Thus, it's desirable to make it easier to achieve
 *		consistency between index scan results and sequential scan results.
 *		So, we throw as less errors as possible.  Regarding this function,
 *		such behavior also matches behavior of JSON_EXISTS() clause of
 *		SQL/JSON.  Regarding vpack_path_match(), this function doesn't have
 *		an analogy in SQL/JSON, so we define its behavior on our own.
 */
PG_FUNCTION_INFO_V1(vpack_path_exists);
Datum
vpack_path_exists(PG_FUNCTION_ARGS) /* OK */
{
	Vpack	   *jb = PG_GETARG_VPACK(0);
	VpackPath  *jp = PG_GETARG_VPACKPATH_P(1);
	VpackPathExecResult res;
	Vpack	   *vars = NULL;
	bool		silent = true;

	if (PG_NARGS() == 4)
	{
		vars = PG_GETARG_VPACK(2);
		silent = PG_GETARG_BOOL(3);
	}

	res = executeVpackPath(jp, vars, jb, !silent, NULL);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (jperIsError(res))
		PG_RETURN_NULL();

	PG_RETURN_BOOL(res == jperOk);
}

/*
 * vpack_path_exists_opr
 *		Implementation of operator "Vpack @? vpackpath" (2-argument version of
 *		vpack_path_exists()).
 */
PG_FUNCTION_INFO_V1(vpack_path_exists_opr);
Datum
vpack_path_exists_opr(PG_FUNCTION_ARGS) /* OK */
{
	/* just call the other one -- it can handle both cases */
	return vpack_path_exists(fcinfo);
}

/*
 * vpack_path_match
 *		Returns vpackpath predicate result item for the specified Vpack value.
 *		See vpack_path_exists() comment for details regarding error handling.
 */
PG_FUNCTION_INFO_V1(vpack_path_match);
Datum
vpack_path_match(PG_FUNCTION_ARGS) /* OK */
{
	Vpack	   *jb = PG_GETARG_VPACK(0);
	VpackPath  *jp = PG_GETARG_VPACKPATH_P(1);
	VpackValueList found = {0};
	Vpack	   *vars = NULL;
	bool		silent = true;

	if (PG_NARGS() == 4)
	{
		vars = PG_GETARG_VPACK(2);
		silent = PG_GETARG_BOOL(3);
	}

	(void) executeVpackPath(jp, vars, jb, !silent, &found);

	PG_FREE_IF_COPY(jb, 0);
	PG_FREE_IF_COPY(jp, 1);

	if (VpackValueListLength(&found) == 1)
	{
		VpackSlice *jbv = VpackValueListHead(&found);

		if(jbv->isBool()) {
			PG_RETURN_BOOL(jbv->getBool());
		}

		if(jbv->isNull()) {
			PG_RETURN_NULL();
		}
	}

	if (!silent)
		ereport(ERROR,
				(errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
				 errmsg("single boolean result is expected")));

	PG_RETURN_NULL();
}

/*
 * vpack_path_match_opr
 *		Implementation of operator "Vpack @@ vpackpath" (2-argument version of
 *		vpack_path_match()).
 */
PG_FUNCTION_INFO_V1(vpack_path_match_opr);
Datum
vpack_path_match_opr(PG_FUNCTION_ARGS) /* OK */
{
	/* just call the other one -- it can handle both cases */
	return vpack_path_match(fcinfo);
}

/*
 * vpack_path_query
 *		Executes vpackpath for given Vpack document and returns result as
 *		rowset.
 */
PG_FUNCTION_INFO_V1(vpack_path_query);
Datum
vpack_path_query(PG_FUNCTION_ARGS) /* OK */
{
	FuncCallContext *funcctx;
	List	   *found;
	VpackSlice *v;
	ListCell   *c;

	if (SRF_IS_FIRSTCALL())
	{
		VpackPath  *jp;
		Vpack	   *jb;
		MemoryContext oldcontext;
		Vpack	   *vars;
		bool		silent;
		VpackValueList found = {0};

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		jb = PG_GETARG_VPACK_COPY(0);
		jp = PG_GETARG_VPACKPATH_P_COPY(1);
		vars = PG_GETARG_VPACK_COPY(2);
		silent = PG_GETARG_BOOL(3);

		(void) executeVpackPath(jp, vars, jb, !silent, &found);

		funcctx->user_fctx = VpackValueListGetList(&found);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	found = (List*)funcctx->user_fctx;

	c = list_head(found);

	if (c == NULL)
		SRF_RETURN_DONE(funcctx);

	v = (VpackSlice*) lfirst(c);
	funcctx->user_fctx = list_delete_first(found);

	SRF_RETURN_NEXT(funcctx, VpackGetDatum(slice_to_vpack(*v)));
}

/*
 * vpack_path_query_array
 *		Executes vpackpath for given Vpack document and returns result as
 *		Vpack array.
 */
PG_FUNCTION_INFO_V1(vpack_path_query_array);
Datum
vpack_path_query_array(PG_FUNCTION_ARGS) /* OK */
{
	Vpack	   *jb = PG_GETARG_VPACK(0);
	VpackPath  *jp = PG_GETARG_VPACKPATH_P(1);
	VpackValueList found = {0};
	Vpack	   *vars = PG_GETARG_VPACK(2);
	bool		silent = PG_GETARG_BOOL(3);

	(void) executeVpackPath(jp, vars, jb, !silent, &found);

	PG_RETURN_VPACK(slice_to_vpack(*wrapItemsInArray(&found)));
}

/*
 * vpack_path_query_first
 *		Executes vpackpath for given jsonb document and returns first result
 *		item.  If there are no items, NULL returned.
 */
PG_FUNCTION_INFO_V1(vpack_path_query_first);
Datum
vpack_path_query_first(PG_FUNCTION_ARGS) /* OK */
{
	Vpack	   *jb = PG_GETARG_VPACK(0);
	VpackPath  *jp = PG_GETARG_VPACKPATH_P(1);
	VpackValueList found = {0};
	Vpack	   *vars = PG_GETARG_VPACK(2);
	bool		silent = PG_GETARG_BOOL(3);

	(void) executeVpackPath(jp, vars, jb, !silent, &found);

	if (VpackValueListLength(&found) >= 1)
		PG_RETURN_VPACK(slice_to_vpack(*VpackValueListHead(&found)));
	else
		PG_RETURN_NULL();
}

/********************Execute functions for VpackPath**************************/

/*
 * Interface to vpackpath executor
 *
 * 'path' - vpackpath to be executed
 * 'vars' - variables to be substituted to vpackpath
 * 'json' - target document for vpackpath evaluation
 * 'throwErrors' - whether we should throw suppressible errors
 * 'result' - list to store result items into
 *
 * Returns an error if a recoverable error happens during processing, or NULL
 * on no error.
 *
 * Note, Vpack and vpackpath values should be available and untoasted during
 * work because VpackPathItem, VpackSlice and result item could have pointers
 * into input values.  If caller needs to just check if document matches
 * vpackpath, then it doesn't provide a result arg.  In this case executor
 * works till first positive result and does not check the rest if possible.
 * In other case it tries to find all the satisfied result items.
 */
static VpackPathExecResult
executeVpackPath(VpackPath *path, Vpack *vars, Vpack *vpack, bool throwErrors,
				VpackValueList *result) /* OK */
{
	VpackPathExecContext cxt;
	VpackPathExecResult res;
	VpackPathItem jsp;
	VpackSlice	jbv = vpack_to_slice(vpack);
	VpackSlice	varsSlice = vpack_to_slice(vars);

	jspInit(&jsp, path);

	if (vars && !varsSlice.isObject())
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"vars\" argument is not an object"),
				 errdetail("Vpackpath parameters should be encoded as key-value pairs of \"vars\" object.")));
	}

	cxt.vars = vars;
	cxt.laxMode = (path->header & VPACKPATH_LAX) != 0;
	cxt.ignoreStructuralErrors = cxt.laxMode;
	cxt.root = &jbv;
	cxt.current = &jbv;
	cxt.baseObject.jbc = NULL;
	cxt.baseObject.id = 0;
	cxt.lastGeneratedObjectId = vars ? 2 : 1;
	cxt.innermostArraySize = -1;
	cxt.throwErrors = throwErrors;

	if (jspStrictAbsenseOfErrors(&cxt) && !result)
	{
		/*
		 * In strict mode we must get a complete list of values to check that
		 * there are no errors at all.
		 */
		VpackValueList vals = {0};

		res = executeItem(&cxt, &jsp, &jbv, &vals);

		if (jperIsError(res))
			return res;

		return VpackValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	res = executeItem(&cxt, &jsp, &jbv, result);

	Assert(!throwErrors || !jperIsError(res));

	return res;
}

/*
 * Execute vpackpath with automatic unwrapping of current item in lax mode.
 */
static VpackPathExecResult
executeItem(VpackPathExecContext *cxt, VpackPathItem *jsp,
			VpackSlice *jb, VpackValueList *found) /* OK */
{
	return executeItemOptUnwrapTarget(cxt, jsp, jb, found, jspAutoUnwrap(cxt));
}

/*
 * Main vpackpath executor function: walks on vpackpath structure, finds
 * relevant parts of Vpack and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
static VpackPathExecResult
executeItemOptUnwrapTarget(VpackPathExecContext *cxt, VpackPathItem *jsp,
						   VpackSlice *jb, VpackValueList *found, bool unwrap) /* OK */
{
	VpackPathItem elem;
	VpackPathExecResult res = jperNotFound;
	VpackBaseObjectInfo baseObject;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch (jsp->type)
	{
			/* all boolean item types: */
		case jpiAnd:
		case jpiOr:
		case jpiNot:
		case jpiIsUnknown:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiExists:
		case jpiStartsWith:
		case jpiLikeRegex:
			{
				VpackPathBool st = executeBoolItem(cxt, jsp, jb, true);

				res = appendBoolResult(cxt, jsp, found, st);
				break;
			}

		case jpiKey:
			if (jb->isObject())
			{
				int32 keyLen;
				char* key = jspGetString(jsp, &keyLen);

				VpackSlice vv = jb->get(key);
				VpackSlice *v = copyVpackSlice(&vv);

				if (!vv.isNone())
				{
					res = executeNextItem(cxt, jsp, NULL,
										  v, found, false);

					/* free value if it was not added to found list */
					if (jspHasNext(jsp) || !found)
						pfree(v);
				}
				else if (!jspIgnoreStructuralErrors(cxt))
				{
					Assert(found);

					if (!jspThrowErrors(cxt))
						return jperError;

					ereport(ERROR,
							(errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND), \
							 errmsg("VPack object does not contain key \"%s\"",
									pnstrdup(key, keyLen))));
				}
			}
			else if (unwrap && jb->isArray())
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
									  errmsg("vpackpath member accessor can only be applied to an object"))));
			}
			break;

		case jpiRoot:
			jb = cxt->root;
			baseObject = setBaseObject(cxt, jb, 0);
			res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			cxt->baseObject = baseObject;
			break;

		case jpiCurrent:
			res = executeNextItem(cxt, jsp, NULL, cxt->current,
								  found, true);
			break;

		case jpiAnyArray:
			if (jb->isArray())
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				res = executeItemUnwrapTargetArray(cxt, hasNext ? &elem : NULL,
												   jb, found, jspAutoUnwrap(cxt));
			}
			else if (jspAutoWrap(cxt))
				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			else if (!jspIgnoreStructuralErrors(cxt))
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("vpackpath wildcard array accessor can only be applied to an array"))));
			break;

		case jpiIndexArray:
			if (jb->isArray() || jspAutoWrap(cxt))
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = jb->isArray() ? jb->length() : -1;
				bool		singleton = size < 0;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (singleton)
					size = 1;

				cxt->innermostArraySize = size; /* for LAST evaluation */

				for (i = 0; i < jsp->content.array.nelems; i++)
				{
					VpackPathItem from;
					VpackPathItem to;
					int32		index;
					int32		index_from;
					int32		index_to;
					bool		range = jspGetArraySubscript(jsp, &from,
															 &to, i);

					res = getArrayIndex(cxt, &from, jb, &index_from);

					if (jperIsError(res))
						break;

					if (range)
					{
						res = getArrayIndex(cxt, &to, jb, &index_to);

						if (jperIsError(res))
							break;
					}
					else
						index_to = index_from;

					if (!jspIgnoreStructuralErrors(cxt) &&
						(index_from < 0 ||
						 index_from > index_to ||
						 index_to >= size))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
											  errmsg("vpackpath array subscript is out of bounds"))));

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						VpackSlice *v;
						bool		copy;

						if (singleton)
						{
							v = jb;
							copy = true;
						}
						else
						{
							VpackSlice s = jb->at((uint32) index);

							if (s.isNone())
								continue;

							v = copyVpackSlice(&s);

							copy = false;
						}

						if (!hasNext && !found)
							return jperOk;

						res = executeNextItem(cxt, jsp, &elem, v, found,
											  copy);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("vpackpath array accessor can only be applied to an array"))));
			}
			break;

		case jpiLast:
			{
				VpackBuffer<uint8_t> tmpjbv;
				VpackBuffer<uint8_t> *lastjbv;
				int			last;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (cxt->innermostArraySize < 0)
					elog(ERROR, "evaluating vpackpath LAST outside of array subscript");

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				lastjbv = hasNext ? &tmpjbv : new VpackBuffer<uint8_t>();

				/* XXX */
				VpackBuilder *b = new VpackBuilder(*lastjbv);
				b->add(VpackValue(last));
				VpackSlice *v = new VpackSlice(b->start());

				res = executeNextItem(cxt, jsp, &elem,
									  v, found, hasNext);
			}
			break;

		case jpiAnyKey:
			if (jb->isObject())
			{
				bool		hasNext = jspGetNext(jsp, &elem);

//				if (jb->type != jbvBinary)
//					elog(ERROR, "invalid jsonb object type: %d", jb->type);

				return executeAnyItem
					(cxt, hasNext ? &elem : NULL,
					 jb, found, 1, 1, 1,
					 false, jspAutoUnwrap(cxt));
			}
			else if (unwrap && jb->isArray())
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
									  errmsg("vpackpath wildcard member accessor can only be applied to an object"))));
			}
			break;

		case jpiAdd:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_add_opt_error, found);

		case jpiSub:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_sub_opt_error, found);

		case jpiMul:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mul_opt_error, found);

		case jpiDiv:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_div_opt_error, found);

		case jpiMod:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mod_opt_error, found);

		case jpiPlus:
			return executeUnaryArithmExpr(cxt, jsp, jb, NULL, found);

		case jpiMinus:
			return executeUnaryArithmExpr(cxt, jsp, jb, numeric_uminus,
										  found);

		case jpiFilter:
			{
				VpackPathBool st;

				if (unwrap && jb->isArray())
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				jspGetArg(jsp, &elem);
				st = executeNestedBoolItem(cxt, &elem, jb);
				if (st != jpbTrue)
					res = jperNotFound;
				else
					res = executeNextItem(cxt, jsp, NULL,
										  jb, found, true);
				break;
			}

		case jpiAny:
			{
				bool		hasNext = jspGetNext(jsp, &elem);

				/* first try without any intermediate steps */
				if (jsp->content.anybounds.first == 0)
				{
					bool		savedIgnoreStructuralErrors;

					savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
					cxt->ignoreStructuralErrors = true;
					res = executeNextItem(cxt, jsp, &elem,
										  jb, found, true);
					cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

					if (res == jperOk && !found)
						break;
				}

				if(jb->isArray() || jb->isObject()) // (jb->type == jbvBinary)
				{
					res = executeAnyItem
						(cxt, hasNext ? &elem : NULL,
						 jb, found,
						 1,
						 jsp->content.anybounds.first,
						 jsp->content.anybounds.last,
						 true, jspAutoUnwrap(cxt));
				}

				break;
			}

		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			{
				VpackBuffer<uint8_t> vbuf;
				VpackBuffer<uint8_t> *v;
				bool		hasNext = jspGetNext(jsp, &elem);

				if (!hasNext && !found)
				{
					res = jperOk;	/* skip evaluation */
					break;
				}

				v = hasNext ? &vbuf : new VpackBuffer<uint8_t>();

				baseObject = cxt->baseObject;

				getVpackPathItem(cxt, jsp, v);

				/* XXX */
				VpackSlice *vv = new VpackSlice(v->data());

				res = executeNextItem(cxt, jsp, &elem,
									  vv, found, hasNext);
				cxt->baseObject = baseObject;
			}
			break;

		case jpiType:
			{
				VpackBuffer<uint8_t> *buf = new VpackBuffer<uint8_t>();
				VpackBuilder *b = new VpackBuilder(*buf);

				char* typeName;

				switch(jb->value().type())
				{
					// object, array, number, string, bolean, null, unknown
					case VpackValueType::Null:
						typeName = (char*)"null";
						break;
					case VpackValueType::Bool:
						typeName = (char*)"boolean";
						break;
					case VpackValueType::Array:
						typeName = (char*)"array";
						break;
					case VpackValueType::Object:
						typeName = (char*)"object";
						break;
					case VpackValueType::Double:
					case VpackValueType::Int:
					case VpackValueType::UInt:
					case VpackValueType::SmallInt:
					case VpackValueType::BCD:
						typeName = (char*)"number";
						break;
					case VpackValueType::UTCDate:
						typeName = (char*)"number";
						break;
					case VpackValueType::String:
						typeName = (char*)"string";
						break;
					case VpackValueType::Binary:
						typeName = (char*)"binary";
						break;
					case VpackValueType::None:
					case VpackValueType::Illegal:
						typeName = (char*)"unknown";
						break;
					case VpackValueType::External:
					case VpackValueType::MinKey:
					case VpackValueType::MaxKey:
					case VpackValueType::Custom:
					case VpackValueType::Tagged:
						typeName = (char*)"unknown";
						break;
					default:
						typeName = (char*)"unknown";
						break;
				}

				b->add(VpackValue(typeName));

				VpackSlice *jbv = new VpackSlice(b->start());

				res = executeNextItem(cxt, jsp, NULL, jbv,
									  found, false);
			}
			break;

		case jpiSize:
			{
				int			size;

				// be consistent with jsonpath even though we could also return the
				// number of elements in an object
				if(jb->isArray())
				{
					size = jb->length();
				}
				else
				{
					size = -1;
				}

				if (size < 0)
				{
					if (!jspAutoWrap(cxt))
					{
						if (!jspIgnoreStructuralErrors(cxt))
							RETURN_ERROR(ereport(ERROR,
												 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
												  errmsg("vpackpath item method .%s() can only be applied to an array",
														 jspOperationName(jsp->type)))));
						break;
					}

					size = 1;
				}

				/* XXX */
				VpackBuffer<uint8_t> *buf = new VpackBuffer<uint8_t>();
				VpackBuilder *b = new VpackBuilder(*buf);
				b->add(VpackValue(size));

				jb = new VpackSlice(b->start());

				res = executeNextItem(cxt, jsp, NULL, jb, found, false);
			}
			break;

		case jpiAbs:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_abs,
											found);

		case jpiFloor:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_floor,
											found);

		case jpiCeiling:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_ceil,
											found);

		case jpiDouble:
			{
				VpackBuffer<uint8_t> jbv;

				if (unwrap && jb->isArray())
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				if (jb->isNumber())
				{
					/* XXX */
					VpackBuffer<char> buffer;
				    VpackCharBufferSink sink(&buffer);
				    VpackDumper::dump(jb, &sink, &dumperOptions);

					char       *tmp = pnstrdup(buffer.data(), buffer.size());
					bool		have_error = false;

					(void) float8in_internal_opt_error(tmp,
													   NULL,
													   "double precision",
													   tmp,
													   &have_error);

					if (have_error)
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("vpackpath item method .%s() can only be applied to a numeric value",
													 jspOperationName(jsp->type)))));
					res = jperOk;
				}
				else if (jb->isString())
				{
					VpackValueLength l;
					const char *tmpStr = jb->getString(l);

					/* cast string as double */
					double		val;
					char	   *tmp = pnstrdup(tmpStr, l);
					bool		have_error = false;

					val = float8in_internal_opt_error(tmp,
													  NULL,
													  "double precision",
													  tmp,
													  &have_error);

					if (have_error || isinf(val))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("vpackpath item method .%s() can only be applied to a numeric value",
													 jspOperationName(jsp->type)))));

					VpackBuilder *b = new VpackBuilder(jbv);
					b->add(VpackValue(val, VpackValueType::Double));

					jb = new VpackSlice(b->start());

					res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("vpackpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));

				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiKeyValue:
			if (unwrap && jb->isArray())
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

			return executeKeyValueMethod(cxt, jsp, jb, found);

		default:
			elog(ERROR, "unrecognized vpackpath item type: %d", jsp->type);
	}

	return res;
}

/*
 * Unwrap current array item and execute vpackpath for each of its elements.
 */
static VpackPathExecResult
executeItemUnwrapTargetArray(VpackPathExecContext *cxt, VpackPathItem *jsp,
							 VpackSlice *jb, VpackValueList *found,
							 bool unwrapElements) /* OK */
{
	if (!jb->isArray() && !jb->isObject())
	{
		Assert(!jb->isArray());
		elog(ERROR, "invalid vpack array value type: %d", jb->typeName());
	}

	return executeAnyItem
		(cxt, jsp, jb, found, 1, 1, 1,
		 false, unwrapElements);
}

/*
 * Execute next vpackpath item if exists.  Otherwise put "v" to the "found"
 * list if provided.
 */
static VpackPathExecResult
executeNextItem(VpackPathExecContext *cxt,
				VpackPathItem *cur, VpackPathItem *next,
				VpackSlice *v, VpackValueList *found, bool copy) /* OK */
{
	VpackPathItem elem;
	bool		hasNext;

	if (!cur)
		hasNext = next != NULL;
	else if (next)
		hasNext = jspHasNext(cur);
	else
	{
		next = &elem;
		hasNext = jspGetNext(cur, next);
	}

	if (hasNext)
		return executeItem(cxt, next, v, found);

	if (found)
		VpackValueListAppend(found, copy ? copyVpackSlice(v) : v);

	return jperOk;
}

/*
 * Same as executeItem(), but when "unwrap == true" automatically unwraps
 * each array item from the resulting sequence in lax mode.
 */
static VpackPathExecResult
executeItemOptUnwrapResult(VpackPathExecContext *cxt, VpackPathItem *jsp,
						   VpackSlice *jb, bool unwrap,
						   VpackValueList *found) /* OK */
{
	if (unwrap && jspAutoUnwrap(cxt))
	{
		VpackValueList seq = {0};
		VpackValueListIterator it;
		VpackPathExecResult res = executeItem(cxt, jsp, jb, &seq);
		VpackSlice *item;

		if (jperIsError(res))
			return res;

		VpackValueListInitIterator(&seq, &it);
		while ((item = VpackValueListNext(&seq, &it)))
		{
			if (item->isArray())
				executeItemUnwrapTargetArray(cxt, NULL, item, found, false);
			else
				VpackValueListAppend(found, item);
		}

		return jperOk;
	}

	return executeItem(cxt, jsp, jb, found);
}

/*
 * Same as executeItemOptUnwrapResult(), but with error suppression.
 */
static VpackPathExecResult
executeItemOptUnwrapResultNoThrow(VpackPathExecContext *cxt,
								  VpackPathItem *jsp,
								  VpackSlice *jb, bool unwrap,
								  VpackValueList *found) /* OK */
{
	VpackPathExecResult res;
	bool		throwErrors = cxt->throwErrors;

	cxt->throwErrors = false;
	res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
	cxt->throwErrors = throwErrors;

	return res;
}

/* Execute boolean-valued vpackpath expression. */
static VpackPathBool
executeBoolItem(VpackPathExecContext *cxt, VpackPathItem *jsp,
				VpackSlice *jb, bool canHaveNext) /* OK */
{
	VpackPathItem larg;
	VpackPathItem rarg;
	VpackPathBool res;
	VpackPathBool res2;

	if (!canHaveNext && jspHasNext(jsp))
		elog(ERROR, "boolean vpackpath item cannot have next item");

	switch (jsp->type)
	{
		case jpiAnd:
			jspGetLeftArg(jsp, &larg);
			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbFalse)
				return jpbFalse;

			/*
			 * SQL/JSON says that we should check second arg in case of
			 * jperError
			 */

			jspGetRightArg(jsp, &rarg);
			res2 = executeBoolItem(cxt, &rarg, jb, false);

			return res2 == jpbTrue ? res : res2;

		case jpiOr:
			jspGetLeftArg(jsp, &larg);
			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbTrue)
				return jpbTrue;

			jspGetRightArg(jsp, &rarg);
			res2 = executeBoolItem(cxt, &rarg, jb, false);

			return res2 == jpbFalse ? res : res2;

		case jpiNot:
			jspGetArg(jsp, &larg);

			res = executeBoolItem(cxt, &larg, jb, false);

			if (res == jpbUnknown)
				return jpbUnknown;

			return res == jpbTrue ? jpbFalse : jpbTrue;

		case jpiIsUnknown:
			jspGetArg(jsp, &larg);
			res = executeBoolItem(cxt, &larg, jb, false);
			return res == jpbUnknown ? jpbTrue : jpbFalse;

		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			jspGetLeftArg(jsp, &larg);
			jspGetRightArg(jsp, &rarg);
			return executePredicate(cxt, jsp, &larg, &rarg, jb, true,
									executeComparison, NULL);

		case jpiStartsWith:		/* 'whole STARTS WITH initial' */
			jspGetLeftArg(jsp, &larg);	/* 'whole' */
			jspGetRightArg(jsp, &rarg); /* 'initial' */
			return executePredicate(cxt, jsp, &larg, &rarg, jb, false,
									executeStartsWith, NULL);

		case jpiLikeRegex:		/* 'expr LIKE_REGEX pattern FLAGS flags' */
			{
				/*
				 * 'expr' is a sequence-returning expression.  'pattern' is a
				 * regex string literal.  SQL/JSON standard requires XQuery
				 * regexes, but we use Postgres regexes here.  'flags' is a
				 * string literal converted to integer flags at compile-time.
				 */
				VpackLikeRegexContext lrcxt = {0};

				jspInitByBuffer(&larg, jsp->base,
								jsp->content.like_regex.expr);

				return executePredicate(cxt, jsp, &larg, NULL, jb, false,
										executeLikeRegex, &lrcxt);
			}

		case jpiExists:
			jspGetArg(jsp, &larg);

			if (jspStrictAbsenseOfErrors(cxt))
			{
				/*
				 * In strict mode we must get a complete list of values to
				 * check that there are no errors at all.
				 */
				VpackValueList vals = {0};
				VpackPathExecResult res =
				executeItemOptUnwrapResultNoThrow(cxt, &larg, jb,
												  false, &vals);

				if (jperIsError(res))
					return jpbUnknown;

				return VpackValueListIsEmpty(&vals) ? jpbFalse : jpbTrue;
			}
			else
			{
				VpackPathExecResult res =
				executeItemOptUnwrapResultNoThrow(cxt, &larg, jb,
												  false, NULL);

				if (jperIsError(res))
					return jpbUnknown;

				return res == jperOk ? jpbTrue : jpbFalse;
			}

		default:
			elog(ERROR, "invalid boolean vpackpath item type: %d", jsp->type);
			return jpbUnknown;
	}
}

/*
 * Execute nested (filters etc.) boolean expression pushing current SQL/JSON
 * item onto the stack.
 */
static VpackPathBool
executeNestedBoolItem(VpackPathExecContext *cxt, VpackPathItem *jsp,
					  VpackSlice *jb) /* OK */
{
	VpackSlice *prev;
	VpackPathBool res;

	prev = cxt->current;
	cxt->current = jb;
	res = executeBoolItem(cxt, jsp, jb, false);
	cxt->current = prev;

	return res;
}

/*
 * Implementation of several vpackpath nodes:
 *  - jpiAny (.** accessor),
 *  - jpiAnyKey (.* accessor),
 *  - jpiAnyArray ([*] accessor)
 */
static VpackPathExecResult
executeAnyItem(VpackPathExecContext *cxt, VpackPathItem *jsp, VpackSlice *jbc,
			   VpackValueList *found, uint32 level, uint32 first, uint32 last,
			   bool ignoreStructuralErrors, bool unwrapNext) /* OK */
{
	VpackPathExecResult res = jperNotFound;
	int32		r;

	check_stack_depth();

	if (level > last)
		return res;

	if(jbc->isObject())
	{
		for(auto const& it : VpackObjectIterator(*jbc))
		{
			VpackSlice	v = it.value;

			if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 (!v.isArray() && !v.isObject())))	/* leaves only requested */ /* v.type != jbvBinary */
			{
				/* check expression */
				if (jsp)
				{
					if (ignoreStructuralErrors)
					{
						bool		savedIgnoreStructuralErrors;

						savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
						cxt->ignoreStructuralErrors = true;
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
						cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;
					}
					else
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}
				else if (found)
					VpackValueListAppend(found, copyVpackSlice(&v));
				else
					return jperOk;
			}

			if (level < last && (v.isObject() || v.isArray())) /* v.type == jbvBinary */
			{
				res = executeAnyItem
					(cxt, jsp, &v, found, /* XXX &v */
					 level + 1, first, last,
					 ignoreStructuralErrors, unwrapNext);

				if (jperIsError(res))
					break;

				if (res == jperOk && found == NULL)
					break;
			}
		}
	}
	else if(jbc->isArray())
	{
		for(auto const& it : VpackArrayIterator(*jbc))
		{
			VpackSlice	v = it;

			if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 (!v.isArray() && !v.isObject())))	/* leaves only requested */ /* v.type != jbvBinary */
			{
				/* check expression */
				if (jsp)
				{
					if (ignoreStructuralErrors)
					{
						bool		savedIgnoreStructuralErrors;

						savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
						cxt->ignoreStructuralErrors = true;
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
						cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;
					}
					else
						res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}
				else if (found)
					VpackValueListAppend(found, copyVpackSlice(&v));
				else
					return jperOk;
			}

			if (level < last && (v.isObject() || v.isArray()))
			{
				res = executeAnyItem
					(cxt, jsp, &v, found, /* XXX &v */
					 level + 1, first, last,
					 ignoreStructuralErrors, unwrapNext);

				if (jperIsError(res))
					break;

				if (res == jperOk && found == NULL)
					break;
			}
		}
	}

	return res;
}

/*
 * Execute unary or binary predicate.
 *
 * Predicates have existence semantics, because their operands are item
 * sequences.  Pairs of items from the left and right operand's sequences are
 * checked.  TRUE returned only if any pair satisfying the condition is found.
 * In strict mode, even if the desired pair has already been found, all pairs
 * still need to be examined to check the absence of errors.  If any error
 * occurs, UNKNOWN (analogous to SQL NULL) is returned.
 */
static VpackPathBool
executePredicate(VpackPathExecContext *cxt, VpackPathItem *pred,
				 VpackPathItem *larg, VpackPathItem *rarg, VpackSlice *jb,
				 bool unwrapRightArg, VpackPathPredicateCallback exec,
				 void *param) /* OK */
{
	VpackPathExecResult res;
	VpackValueListIterator lseqit;
	VpackValueList lseq = {0};
	VpackValueList rseq = {0};
	VpackSlice *lval;
	bool		error = false;
	bool		found = false;

	/* Left argument is always auto-unwrapped. */
	res = executeItemOptUnwrapResultNoThrow(cxt, larg, jb, true, &lseq);
	if (jperIsError(res))
		return jpbUnknown;

	if (rarg)
	{
		/* Right argument is conditionally auto-unwrapped. */
		res = executeItemOptUnwrapResultNoThrow(cxt, rarg, jb,
												unwrapRightArg, &rseq);
		if (jperIsError(res))
			return jpbUnknown;
	}

	VpackValueListInitIterator(&lseq, &lseqit);
	while ((lval = VpackValueListNext(&lseq, &lseqit)))
	{
		VpackValueListIterator rseqit;
		VpackSlice *rval;
		bool		first = true;

		VpackValueListInitIterator(&rseq, &rseqit);
		if (rarg)
			rval = VpackValueListNext(&rseq, &rseqit);
		else
			rval = NULL;

		/* Loop over right arg sequence or do single pass otherwise */
		while (rarg ? (rval != NULL) : first)
		{
			VpackPathBool res = exec(pred, lval, rval, param);

			if (res == jpbUnknown)
			{
				if (jspStrictAbsenseOfErrors(cxt))
					return jpbUnknown;

				error = true;
			}
			else if (res == jpbTrue)
			{
				if (!jspStrictAbsenseOfErrors(cxt))
					return jpbTrue;

				found = true;
			}

			first = false;
			if (rarg)
				rval = VpackValueListNext(&rseq, &rseqit);
		}
	}

	if (found)					/* possible only in strict mode */
		return jpbTrue;

	if (error)					/* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

/*
 * Execute binary arithmetic expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
static VpackPathExecResult
executeBinaryArithmExpr(VpackPathExecContext *cxt, VpackPathItem *jsp,
						VpackSlice *jb, BinaryArithmFunc func,
						VpackValueList *found) /* OK */
{
	VpackPathExecResult jper;
	VpackPathItem elem;
	VpackValueList lseq = {0};
	VpackValueList rseq = {0};
	VpackSlice *lval;
	VpackSlice *rval;
	Numeric		res;

	jspGetLeftArg(jsp, &elem);

	/*
	 * XXX: By standard only operands of multiplicative expressions are
	 * unwrapped.  We extend it to other binary arithmetic expressions too.
	 */
	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &lseq);
	if (jperIsError(jper))
		return jper;

	jspGetRightArg(jsp, &elem);

	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &rseq);
	if (jperIsError(jper))
		return jper;

	if (VpackValueListLength(&lseq) != 1 ||
		!(lval = getScalar(VpackValueListHead(&lseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("left operand of vpackpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	if (VpackValueListLength(&rseq) != 1 ||
		!(rval = getScalar(VpackValueListHead(&rseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("right operand of vpackpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	Numeric l = VpackToNumeric(*lval);
	Numeric r = VpackToNumeric(*rval);

	if (jspThrowErrors(cxt))
	{
		res = func(l, r, NULL);
	}
	else
	{
		bool		error = false;

		res = func(l, r, &error);

		if (error)
			return jperError;
	}

	if (!jspGetNext(jsp, &elem) && !found)
		return jperOk;

	lval = NumericToVpack(res);

	return executeNextItem(cxt, jsp, &elem, lval, found, false);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static VpackPathExecResult
executeUnaryArithmExpr(VpackPathExecContext *cxt, VpackPathItem *jsp,
					   VpackSlice *jb, PGFunction func, VpackValueList *found) /* OK */
{
	VpackPathExecResult jper;
	VpackPathExecResult jper2;
	VpackPathItem elem;
	VpackValueList seq = {0};
	VpackValueListIterator it;
	VpackSlice *val;
	bool		hasNext;

	jspGetArg(jsp, &elem);
	jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &seq);

	if (jperIsError(jper))
		return jper;

	jper = jperNotFound;

	hasNext = jspGetNext(jsp, &elem);

	VpackValueListInitIterator(&seq, &it);
	while ((val = VpackValueListNext(&seq, &it)))
	{
		if ((val = getScalar(val, jbvNumeric)))
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else
		{
			if (!found && !hasNext)
				continue;		/* skip non-numerics processing */

			RETURN_ERROR(ereport(ERROR,
								 (errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND),
								  errmsg("operand of unary vpackpath operator %s is not a numeric value",
										 jspOperationName(jsp->type)))));
		}

		if (func)
		{
			Numeric res = DatumGetNumeric(DirectFunctionCall1(func, NumericGetDatum(VpackToNumeric(*val))));
			VpackSlice *s = NumericToVpack(res); /* XXX */

			val->set(s->start());
		}

		jper2 = executeNextItem(cxt, jsp, &elem, val, found, false);

		if (jperIsError(jper2))
			return jper2;

		if (jper2 == jperOk)
		{
			if (!found)
				return jperOk;
			jper = jperOk;
		}
	}

	return jper;
}

/*
 * STARTS_WITH predicate callback.
 *
 * Check if the 'whole' string starts from 'initial' string.
 */
static VpackPathBool
executeStartsWith(VpackPathItem *jsp, VpackSlice *whole, VpackSlice *initial,
				  void *param) /* OK */
{
	if(!whole->isString() || !initial->isString())
	{
		return jpbUnknown;
	}

	VpackValueLength wholeLength;
	VpackValueLength initialLength;

	const char* wholeStr = whole->getString(wholeLength);
	const char* initialStr = initial->getString(initialLength);

	if (wholeLength >= initialLength && !memcmp(wholeStr, initialStr, initialLength))
		return jpbTrue;

	return jpbFalse;
}

/*
 * LIKE_REGEX predicate callback.
 *
 * Check if the string matches regex pattern.
 */
static VpackPathBool
executeLikeRegex(VpackPathItem *jsp, VpackSlice *str, VpackSlice *rarg,
				 void *param) /* OK */
{
	VpackLikeRegexContext *cxt = (VpackLikeRegexContext*)param;

	if (!str->isString())
		return jpbUnknown;

	/* Cache regex text and converted flags. */
	if (!cxt->regex)
	{
		cxt->regex =
			cstring_to_text_with_len(jsp->content.like_regex.pattern,
									 jsp->content.like_regex.patternlen);
		cxt->cflags = jspConvertRegexFlags(jsp->content.like_regex.flags);
	}

	VpackValueLength len;
	char* val = (char*) str->getString(len);

	if (RE_compile_and_execute(cxt->regex, val, len,
							   cxt->cflags, DEFAULT_COLLATION_OID, 0, NULL))
		return jpbTrue;

	return jpbFalse;
}

/*
 * Execute numeric item methods (.abs(), .floor(), .ceil()) using the specified
 * user function 'func'.
 */
static VpackPathExecResult
executeNumericItemMethod(VpackPathExecContext *cxt, VpackPathItem *jsp,
						 VpackSlice *jb, bool unwrap, PGFunction func,
						 VpackValueList *found) /* OK */
{
	VpackPathItem next;
	Datum		datum;

	if (unwrap && jb->isArray())
		return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

	if (!(jb = getScalar(jb, jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
							  errmsg("vpackpath item method .%s() can only be applied to a numeric value",
									 jspOperationName(jsp->type)))));

	datum = DirectFunctionCall1(func, NumericGetDatum(VpackToNumeric(*jb)));

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;

	jb = NumericToVpack(DatumGetNumeric(datum));

	return executeNextItem(cxt, jsp, &next, jb, found, false);
}

/*
 * Implementation of .keyvalue() method.
 *
 * .keyvalue() method returns a sequence of object's key-value pairs in the
 * following format: '{ "key": key, "value": value, "id": id }'.
 *
 * "id" field is an object identifier which is constructed from the two parts:
 * base object id and its binary offset in base object's Vpack:
 * id = 10000000000 * base_object_id + obj_offset_in_base_object
 *
 * 10000000000 (10^10) -- is a first round decimal number greater than 2^32
 * (maximal offset in Vpack).  Decimal multiplier is used here to improve the
 * readability of identifiers.
 *
 * Base object is usually a root object of the path: context item '$' or path
 * variable '$var', literals can't produce objects for now.  But if the path
 * contains generated objects (.keyvalue() itself, for example), then they
 * become base object for the subsequent .keyvalue().
 *
 * Id of '$' is 0. Id of '$var' is its ordinal (positive) number in the list
 * of variables (see getVpackPathVariable()).  Ids for generated objects
 * are assigned using global counter VpackPathExecContext.lastGeneratedObjectId.
 */
static VpackPathExecResult
executeKeyValueMethod(VpackPathExecContext *cxt, VpackPathItem *jsp,
					  VpackSlice *jb, VpackValueList *found) /* OK */
{
	VpackPathExecResult res = jperNotFound;
	VpackPathItem next;
	int64		id;
	bool		hasNext;

	if (!jb->isObject())
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
							  errmsg("vpackpath item method .%s() can only be applied to an object",
									 jspOperationName(jsp->type)))));

	if (jb->isEmptyObject())
		return jperNotFound;	/* no key-value pairs */

	hasNext = jspGetNext(jsp, &next);

	/* construct object id from its base object and offset inside that */
	id = !jb->isArray() && !jb->isObject() ? 0 :
		(int64) ((char *) jb->start() - (char *) cxt->baseObject.jbc->start());
	id += (int64) cxt->baseObject.id * INT64CONST(10000000000);

	for(auto const& it : VpackObjectIterator(*jb))
	{
		VpackBuilder b;
		VpackBaseObjectInfo baseObject;

		res = jperOk;

		if (!hasNext && !found)
			break;

		b.openObject();
		b.add("id", VpackValue(id));
		b.add("key", it.key);
		b.add("value", it.value);
		b.close();

		VpackSlice obj = VpackSlice(b.start()); /* XXX */

		baseObject = setBaseObject(cxt, &obj, cxt->lastGeneratedObjectId++);

		res = executeNextItem(cxt, jsp, &next, &obj, found, true);

		cxt->baseObject = baseObject;

		if (jperIsError(res))
			return res;

		if (res == jperOk && !found)
			break;
	}

	return res;
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next vpackpath.
 */
static VpackPathExecResult
appendBoolResult(VpackPathExecContext *cxt, VpackPathItem *jsp,
				 VpackValueList *found, VpackPathBool res) /* OK */
{
	VpackPathItem next;
	VpackSlice	jbv;

	if (!jspGetNext(jsp, &next) && !found)
		return jperOk;			/* found singleton boolean value */

	if (res == jpbUnknown)
	{
		jbv = VpackSlice::nullSlice();
	}
	else
	{
		jbv = VpackSlice::booleanSlice(res == jpbTrue);
	}

	return executeNextItem(cxt, jsp, &next, &jbv, found, true);
}

/*
 * Convert vpackpath's scalar or variable node to actual Vpack value.
 *
 * If node is a variable then its id returned, otherwise 0 returned.
 */
static void
getVpackPathItem(VpackPathExecContext *cxt, VpackPathItem *item,
				VpackBuffer<uint8_t> *value) /* OK */
{
	VpackBuilder *b = new VpackBuilder(*value);

	switch (item->type)
	{
		case jpiNull:
			b->add(VpackValue(0, VpackValueType::Null));
			break;
		case jpiBool:
			b->add(VpackValue(jspGetBool(item), VpackValueType::Bool));
			break;
		case jpiNumeric:
			{
				b->add(*NumericToVpack(jspGetNumeric(item)));
				break;
			}
		case jpiString:
			{
				int len;
				char* str = jspGetString(item, &len);
				b->add(VpackValuePair(str, len, VpackValueType::String));
				break;
			}
		case jpiVariable:
			getVpackPathVariable(cxt, item, cxt->vars, value);
			return;
		default:
			elog(ERROR, "unexpected vpackpath item type");
	}
}

/*
 * Get the value of variable passed to vpackpath executor
 */
static void
getVpackPathVariable(VpackPathExecContext *cxt, VpackPathItem *variable,
					Vpack *vars, VpackBuffer<uint8_t> *value) /* OK */
{
	char	   *varName;
	int			varNameLength;
	VpackBuffer<uint8_t> tmp;
	VpackBuilder *b = new VpackBuilder(*value);

	if (!vars)
	{
		b->add(VpackValue(0, VpackValueType::Null));
		return;
	}

	Assert(variable->type == jpiVariable);
	varName = jspGetString(variable, &varNameLength);

	VpackSlice slice = vpack_to_slice(vars);

	VpackSlice v = slice.get(varName, varNameLength);

	if (!v.isNone())
	{
		/* XXX *value = *v */
		value->clear();
		b->add(v);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("could not find vpackpath variable \"%s\"",
						pnstrdup(varName, varNameLength))));
	}

	VpackBuilder *tmpb = new VpackBuilder(tmp);
	tmpb->add(slice);

	setBaseObject(cxt, new VpackSlice(tmpb->start()), 1); /* XXX */
}

/**************** Support functions for VpackPath execution *****************/

/* Comparison predicate callback. */
static VpackPathBool
executeComparison(VpackPathItem *cmp, VpackSlice *lv, VpackSlice *rv, void *p) /* OK */
{
	return compareItems(cmp->type, lv, rv);
}

/*
 * Perform per-byte comparison of two strings.
 */
static int
binaryCompareStrings(const char *s1, int len1,
					 const char *s2, int len2) /* OK */
{
	int			cmp;

	cmp = memcmp(s1, s2, Min(len1, len2));

	if (cmp != 0)
		return cmp;

	if (len1 == len2)
		return 0;

	return len1 < len2 ? -1 : 1;
}

/*
 * Compare two strings in the current server encoding using Unicode codepoint
 * collation.
 */
static int
compareStrings(const char *mbstr1, int mblen1,
			   const char *mbstr2, int mblen2) /* OK */
{
	if (GetDatabaseEncoding() == PG_SQL_ASCII ||
		GetDatabaseEncoding() == PG_UTF8)
	{
		/*
		 * It's known property of UTF-8 strings that their per-byte comparison
		 * result matches codepoints comparison result.  ASCII can be
		 * considered as special case of UTF-8.
		 */
		return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
	}
	else
	{
		char	   *utf8str1,
				   *utf8str2;
		int			cmp,
					utf8len1,
					utf8len2;

		/*
		 * We have to convert other encodings to UTF-8 first, then compare.
		 * Input strings may be not null-terminated and pg_server_to_any() may
		 * return them "as is".  So, use strlen() only if there is real
		 * conversion.
		 */
		utf8str1 = pg_server_to_any(mbstr1, mblen1, PG_UTF8);
		utf8str2 = pg_server_to_any(mbstr2, mblen2, PG_UTF8);
		utf8len1 = (mbstr1 == utf8str1) ? mblen1 : strlen(utf8str1);
		utf8len2 = (mbstr2 == utf8str2) ? mblen2 : strlen(utf8str2);

		cmp = binaryCompareStrings(utf8str1, utf8len1, utf8str2, utf8len2);

		/*
		 * If pg_server_to_any() did no real conversion, then we actually
		 * compared original strings.  So, we already done.
		 */
		if (mbstr1 == utf8str1 && mbstr2 == utf8str2)
			return cmp;

		/* Free memory if needed */
		if (mbstr1 != utf8str1)
			pfree(utf8str1);
		if (mbstr2 != utf8str2)
			pfree(utf8str2);

		/*
		 * When all Unicode codepoints are equal, return result of binary
		 * comparison.  In some edge cases, same characters may have different
		 * representations in encoding.  Then our behavior could diverge from
		 * standard.  However, that allow us to do simple binary comparison
		 * for "==" operator, which is performance critical in typical cases.
		 * In future to implement strict standard conformance, we can do
		 * normalization of input JSON strings.
		 */
		if (cmp == 0)
			return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
		else
			return cmp;
	}
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
static VpackPathBool
compareItems(int32 op, VpackSlice *jb1, VpackSlice *jb2) /* OK */
{
	int			cmp;
	bool		res;

	if (jb1->type() != jb2->type())
	{
		if (jb1->isNull() || jb2->isNull())

			/*
			 * Equality and order comparison of nulls to non-nulls returns
			 * always false, but inequality comparison returns true.
			 */
			return op == jpiNotEqual ? jpbTrue : jpbFalse;

		if (!jb1->isNumber() || !jb2->isNumber()) // allow comparing different number types
		{
			/* Non-null items of different types are not comparable. */
			return jpbUnknown;
		}
	}

	switch (jb1->type())
	{
		case VpackValueType::Null:
			cmp = 0;
			break;
		case VpackValueType::Bool:
			cmp = jb1->getBool() == jb2->getBool() ? 0 : (jb1->getBool() ? 1 : -1);
			break;
		case VpackValueType::Int:
		case VpackValueType::UInt:
		case VpackValueType::SmallInt:
		case VpackValueType::Double:
		case VpackValueType::BCD:
			cmp = compareNumeric(VpackToNumeric(*jb1), VpackToNumeric(*jb2));
			break;
		case VpackValueType::String:
			{
				VpackValueLength s1l;
				char const *s1 = jb1->getString(s1l);
				VpackValueLength s2l;
				char const *s2 = jb2->getString(s2l);

				if (op == jpiEqual)
				{
					return s1l != s2l ||
						memcmp(s1,
							   s2,
							   s1l) ? jpbFalse : jpbTrue;
				}

				cmp = compareStrings(s1, s1l,
									 s2, s2l);
				break;
			}
		case VpackValueType::UTCDate:
			cmp = jb1->getUTCDate() == jb2->getUTCDate() ? 0 : jb1->getUTCDate() > jb2->getUTCDate() ? 1 : -1;
			break;
		default:
			return jpbUnknown;
	}

	switch (op)
	{
		case jpiEqual:
			res = (cmp == 0);
			break;
		case jpiNotEqual:
			res = (cmp != 0);
			break;
		case jpiLess:
			res = (cmp < 0);
			break;
		case jpiGreater:
			res = (cmp > 0);
			break;
		case jpiLessOrEqual:
			res = (cmp <= 0);
			break;
		case jpiGreaterOrEqual:
			res = (cmp >= 0);
			break;
		default:
			elog(ERROR, "unrecognized vpackpath operation: %d", op);
			return jpbUnknown;
	}

	return res ? jpbTrue : jpbFalse;
}

/* Compare two numerics */
static int
compareNumeric(Numeric a, Numeric b) /* OK */
{
	return DatumGetInt32(DirectFunctionCall2(numeric_cmp,
											 NumericGetDatum(a),
											 NumericGetDatum(b)));
}

static VpackSlice *
copyVpackSlice(VpackSlice *src) /* OK */
{
	return new VpackSlice(src->start());
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
static VpackPathExecResult
getArrayIndex(VpackPathExecContext *cxt, VpackPathItem *jsp, VpackSlice *jb,
			  int32 *index) /* OK */
{
	VpackSlice *jbv;
	VpackValueList found = {0};
	VpackPathExecResult res = executeItem(cxt, jsp, jb, &found);
	Datum		numeric_index;
	bool		have_error = false;

	if (jperIsError(res))
		return res;

	if (VpackValueListLength(&found) != 1 ||
		!(jbv = getScalar(VpackValueListHead(&found), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("vpackpath array subscript is not a single numeric value"))));

	numeric_index = DirectFunctionCall2(numeric_trunc,
										NumericGetDatum(VpackToNumeric(*jbv)),
										Int32GetDatum(0));

	*index = numeric_int4_opt_error(DatumGetNumeric(numeric_index),
									&have_error);

	if (have_error)
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("vpackpath array subscript is out of integer range"))));

	return jperOk;
}

/* Save base object and its id needed for the execution of .keyvalue(). */
static VpackBaseObjectInfo
setBaseObject(VpackPathExecContext *cxt, VpackSlice *jbv, int32 id) /* OK */
{
	VpackBaseObjectInfo baseObject = cxt->baseObject;

	cxt->baseObject.jbc = jbv;
	cxt->baseObject.id = id;

	return baseObject;
}

static void
VpackValueListAppend(VpackValueList *jvl, VpackSlice *jbv) /* OK */
{
	if (jvl->singleton)
	{
		jvl->list = list_make2(jvl->singleton, jbv);
		jvl->singleton = NULL;
	}
	else if (!jvl->list)
		jvl->singleton = jbv;
	else
		jvl->list = lappend(jvl->list, jbv);
}

static int
VpackValueListLength(const VpackValueList *jvl) /* OK */
{
	return jvl->singleton ? 1 : list_length(jvl->list);
}

static bool
VpackValueListIsEmpty(VpackValueList *jvl) /* OK */
{
	return !jvl->singleton && list_length(jvl->list) <= 0;
}

static VpackSlice *
VpackValueListHead(VpackValueList *jvl) /* OK */
{
	return jvl->singleton ? jvl->singleton : (VpackSlice*) linitial(jvl->list);
}

static List *
VpackValueListGetList(VpackValueList *jvl) /* OK */
{
	if (jvl->singleton)
		return list_make1(jvl->singleton);

	return jvl->list;
}

static void
VpackValueListInitIterator(const VpackValueList *jvl, VpackValueListIterator *it) /* OK */
{
	if (jvl->singleton)
	{
		it->value = jvl->singleton;
		it->next = NULL;
	}
	else if (list_head(jvl->list) != NULL)
	{
		it->value = (VpackSlice *) linitial(jvl->list);
		it->next = lnext(list_head(jvl->list));
	}
	else
	{
		it->value = NULL;
		it->next = NULL;
	}
}

/*
 * Get the next item from the sequence advancing iterator.
 */
static VpackSlice *
VpackValueListNext(const VpackValueList *jvl, VpackValueListIterator *it) /* OK */
{
	VpackSlice *result = it->value;

	if (it->next)
	{
		it->value = (VpackSlice*) lfirst(it->next);
		it->next = (ListCell*) lnext(it->next);
	}
	else
	{
		it->value = NULL;
	}

	return result;
}

/* Get scalar of given type or NULL on type mismatch */
static VpackSlice *
getScalar(VpackSlice *scalar, enum jbvType type)
{
	switch(type)
	{
		case jbvNull:
			return scalar->isNull() ? scalar : NULL;
		case jbvString:
			return scalar->isString() ? scalar : NULL;
		case jbvNumeric:
			return scalar->isNumber() ? scalar : NULL; /* XXX */
		case jbvBool:
			return scalar->isBoolean() ? scalar : NULL;
		default:
			return NULL;
	}
}

/* Construct a JSON array from the item list */
static VpackSlice *
wrapItemsInArray(const VpackValueList *items) /* OK */
{
	VpackBuffer<uint8_t> *buf = new VpackBuffer<uint8_t>();
	VpackBuilder *b = new VpackBuilder(*buf);

	VpackValueListIterator it;
	VpackSlice *jbv;

	b->openArray();

	VpackValueListInitIterator(items, &it);
	while ((jbv = VpackValueListNext(items, &it)))
	{
		b->add(*jbv);
	}

	b->close();

	return new VpackSlice(b->start());
}

static Numeric
VpackToNumeric(VpackSlice &s) /* OK */
{
	if(s.isInteger())
	{
		return DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int8GetDatum(s.getInt())));
	}
	else if(s.isDouble())
	{
		return DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(s.getDouble())));
	}
	else if(s.isBCD())
	{
		return NULL; // TODO
	}
	else if(s.isString())
	{
		char *str = slice_to_cstring(s);

		return DatumGetNumeric(DirectFunctionCall3(numeric_in, CStringGetDatum(str), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1)));
	}

	elog(ERROR, "invalid vpack type when converting to Numeric: %s", s.typeName());
	return NULL;
}

/* Convert Numeric to VPack. Does not support big values as BCD support in VPack is not implemented. */
static VpackSlice*
NumericToVpack(Numeric n) /* OK */
{
	if(numeric_is_nan(n))
	{
		// TODO: make static

		VpackBuffer<uint8_t> *buf = new VpackBuffer<uint8_t>();
		VpackBuilder *b = new VpackBuilder(*buf);

		b->add(VpackValue(NAN, VpackValueType::Double));

		return new VpackSlice(b->start());
	}

	int32 scale = DatumGetInt32(DirectFunctionCall1(numeric_scale, NumericGetDatum(n)));

	VpackBuffer<uint8_t> *buf = new VpackBuffer<uint8_t>();
	VpackBuilder *b = new VpackBuilder(*buf);

	char *tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(n)));

	if(scale == 0)
	{
		int64 v = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(n)));

		b->add(VpackValue(v));
	}
	else
	{
		double v = DatumGetFloat8(DirectFunctionCall1(numeric_float8_no_overflow, NumericGetDatum(n)));

		b->add(VpackValue(v));
	}

	return new VpackSlice(b->start());
}

} // extern "C"
