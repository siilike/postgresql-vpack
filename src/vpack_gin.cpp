
#include "vpack.h"

extern "C" {

#include "postgres.h"
#include "catalog/pg_type.h"
#include "catalog/pg_collation.h"
#include "utils/array.h"
#include "utils/varlena.h"
#include "access/stratnum.h"
#include "access/gin.h"
#include "access/hash.h"


/*
 * In the standard vpack_ops GIN opclass for vpack, we choose to index both
 * keys and values.  The storage format is text.  The first byte of the text
 * string distinguishes whether this is a key (always a string), null value,
 * boolean value, numeric value, or string value.  However, array elements
 * that are strings are marked as though they were keys; this imprecision
 * supports the definition of the "exists" operator, which treats array
 * elements like keys.  The remainder of the text string is empty for a null
 * value, "t" or "f" for a boolean value, a normalized print representation of
 * a numeric value, or the text of a string value.  However, if the length of
 * this text representation would exceed VGIN_MAXLENGTH bytes, we instead hash
 * the text representation and store an 8-hex-digit representation of the
 * uint32 hash value, marking the prefix byte with an additional bit to
 * distinguish that this has happened.  Hashing long strings saves space and
 * ensures that we won't overrun the maximum entry length for a GIN index.
 * (But VGIN_MAXLENGTH is quite a bit shorter than GIN's limit.  It's chosen
 * to ensure that the on-disk text datum will have a short varlena header.)
 * Note that when any hashed item appears in a query, we must recheck index
 * matches against the heap tuple; currently, this costs nothing because we
 * must always recheck for other reasons.
 */
#define VGINFLAG_KEY	0x01	/* key (or string array element) */
#define VGINFLAG_NULL	0x02	/* null value */
#define VGINFLAG_BOOL	0x03	/* boolean value */
#define VGINFLAG_NUM	0x04	/* numeric value */
#define VGINFLAG_STR	0x05	/* string value (if not an array element) */
#define VGINFLAG_HASHED 0x10	/* OR'd into flag if value was hashed */
#define VGIN_MAXLENGTH	125		/* max length of text part before hashing */


#define VpackContainsStrategyNumber		7
#define VpackExistsStrategyNumber		9
#define VpackExistsAnyStrategyNumber	10
#define VpackExistsAllStrategyNumber	11


typedef struct PathHashStack
{
	uint32		hash;
	struct PathHashStack *parent;
} PathHashStack;

static Datum make_text_key(char flag, const char *str, int len);
static Datum make_scalar_key(const VpackSlice& scalarVal, bool is_key);

void gin_extract_vpack_append(const VpackSlice& slice, int *i, Datum **entries, int *total);
void VpackHashScalarValue(const VpackSlice& scalarVal, uint32 *hash);

void gin_extract_vpack_path_append(const VpackSlice& slice, int *i, Datum **entries, int *total, PathHashStack **stack);


PG_FUNCTION_INFO_V1(gin_compare_vpack);

Datum
gin_compare_vpack(PG_FUNCTION_ARGS)
{
	text	   *arg1 = PG_GETARG_TEXT_PP(0);
	text	   *arg2 = PG_GETARG_TEXT_PP(1);
	int32		result;
	char	   *a1p,
			   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	/* Compare text as bttextcmp does, but always using C collation */
	result = varstr_cmp(a1p, len1, a2p, len2, C_COLLATION_OID);

	PG_FREE_IF_COPY(arg1, 0);
	PG_FREE_IF_COPY(arg2, 1);

	PG_RETURN_INT32(result);
}


PG_FUNCTION_INFO_V1(gin_extract_vpack);

Datum
gin_extract_vpack(PG_FUNCTION_ARGS)
{
	Vpack	   *vpack = PG_GETARG_VPACK(0);
	VpackSlice	slice = vpack_to_slice(vpack);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	int			total = slice.isObject() || slice.isArray() ? 2 * slice.length() : 1;

	int			i = 0;
	Datum	   *entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	entries = (Datum *) palloc(sizeof(Datum) * total);

	gin_extract_vpack_append(slice, &i, &entries, &total);

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

void gin_extract_vpack_ensure_size(int *i, Datum **entries, int *total)
{
	if (*i >= *total)
	{
		*total *= 2;
		*entries = (Datum *) repalloc(*entries, sizeof(Datum) * (*total));
	}
}

void gin_extract_vpack_append_value(const VpackSlice& it, int *i, Datum **entries, int *total, bool isArray)
{
	if(it.isArray() || it.isObject())
	{
		gin_extract_vpack_append(it, i, entries, total);
	}
	else
	{
		gin_extract_vpack_ensure_size(i, entries, total);

		/* Pretend string array elements are keys, see jsonb.h */
		(*entries)[(*i)++] = make_scalar_key(it, isArray && it.isString());
	}
}

void
gin_extract_vpack_append(const VpackSlice& slice, int *i, Datum **entries, int *total)
{
	if(slice.isObject())
	{
		for(auto const& it : VpackObjectIterator(slice))
		{
			gin_extract_vpack_ensure_size(i, entries, total);

			(*entries)[(*i)++] = make_scalar_key(it.key, true);

			gin_extract_vpack_append_value(it.value, i, entries, total, false);
		}
	}
	else if(slice.isArray())
	{
		for(auto const& it : VpackArrayIterator(slice))
		{
			gin_extract_vpack_append_value(it, i, entries, total, true);
		}
	}
	else
	{
		gin_extract_vpack_append_value(slice, i, entries, total, false);
	}
}


PG_FUNCTION_INFO_V1(gin_extract_vpack_query);

Datum
gin_extract_vpack_query(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy == VpackContainsStrategyNumber)
	{
		/* Query is a vpack, so just apply gin_extract_vpack... */
		entries = (Datum *)
			DatumGetPointer(DirectFunctionCall2(gin_extract_vpack,
												PG_GETARG_DATUM(0),
												PointerGetDatum(nentries)));
		/* ...although "contains {}" requires a full index scan */
		if (*nentries == 0)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else if (strategy == VpackExistsStrategyNumber)
	{
		/* Query is a text string, which we treat as a key */
		text	   *query = PG_GETARG_TEXT_PP(0);

		*nentries = 1;
		entries = (Datum *) palloc(sizeof(Datum));
		entries[0] = make_text_key(VGINFLAG_KEY,
								   VARDATA_ANY(query),
								   VARSIZE_ANY_EXHDR(query));
	}
	else if (strategy == VpackExistsAnyStrategyNumber ||
			 strategy == VpackExistsAllStrategyNumber)
	{
		/* Query is a text array; each element is treated as a key */
		ArrayType  *query = PG_GETARG_ARRAYTYPE_P(0);
		Datum	   *key_datums;
		bool	   *key_nulls;
		int			key_count;
		int			i,
					j;

		deconstruct_array(query,
						  TEXTOID, -1, false, 'i',
						  &key_datums, &key_nulls, &key_count);

		entries = (Datum *) palloc(sizeof(Datum) * key_count);

		for (i = 0, j = 0; i < key_count; i++)
		{
			/* Nulls in the array are ignored */
			if (key_nulls[i])
				continue;
			entries[j++] = make_text_key(VGINFLAG_KEY,
										 VARDATA(key_datums[i]),
										 VARSIZE(key_datums[i]) - VARHDRSZ);
		}

		*nentries = j;
		/* ExistsAll with no keys should match everything */
		if (j == 0 && strategy == VpackExistsAllStrategyNumber)
			*searchMode = GIN_SEARCH_MODE_ALL;
	}
	else
	{
		elog(ERROR, "unrecognized strategy number: %d", strategy);
		entries = NULL;			/* keep compiler quiet */
	}

	PG_RETURN_POINTER(entries);
}


PG_FUNCTION_INFO_V1(gin_consistent_vpack);

Datum
gin_consistent_vpack(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy == VpackContainsStrategyNumber)
	{
		/*
		 * We must always recheck, since we can't tell from the index whether
		 * the positions of the matched items match the structure of the query
		 * object.  (Even if we could, we'd also have to worry about hashed
		 * keys and the index's failure to distinguish keys from string array
		 * elements.)  However, the tuple certainly doesn't match unless it
		 * contains all the query keys.
		 */
		*recheck = true;
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else if (strategy == VpackExistsStrategyNumber)
	{
		/*
		 * Although the key is certainly present in the index, we must recheck
		 * because (1) the key might be hashed, and (2) the index match might
		 * be for a key that's not at top level of the JSON object.  For (1),
		 * we could look at the query key to see if it's hashed and not
		 * recheck if not, but the index lacks enough info to tell about (2).
		 */
		*recheck = true;
		res = true;
	}
	else if (strategy == VpackExistsAnyStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		res = true;
	}
	else if (strategy == VpackExistsAllStrategyNumber)
	{
		/* As for plain exists, we must recheck */
		*recheck = true;
		/* ... but unless all the keys are present, we can say "false" */
		for (i = 0; i < nkeys; i++)
		{
			if (!check[i])
			{
				res = false;
				break;
			}
		}
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(gin_triconsistent_vpack);

Datum
gin_triconsistent_vpack(PG_FUNCTION_ARGS)
{
	GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	/*
	 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
	 * corresponds to always forcing recheck in the regular consistent
	 * function, for the reasons listed there.
	 */
	if (strategy == VpackContainsStrategyNumber ||
		strategy == VpackExistsAllStrategyNumber)
	{
		/* All extracted keys must be present */
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_FALSE)
			{
				res = GIN_FALSE;
				break;
			}
		}
	}
	else if (strategy == VpackExistsStrategyNumber ||
			 strategy == VpackExistsAnyStrategyNumber)
	{
		/* At least one extracted key must be present */
		res = GIN_FALSE;
		for (i = 0; i < nkeys; i++)
		{
			if (check[i] == GIN_TRUE ||
				check[i] == GIN_MAYBE)
			{
				res = GIN_MAYBE;
				break;
			}
		}
	}
	else
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	PG_RETURN_GIN_TERNARY_VALUE(res);
}

/*
 *
 * vpack_path_ops GIN opclass support functions
 *
 * In a vpack_path_ops index, the GIN keys are uint32 hashes, one per JSON
 * value; but the JSON key(s) leading to each value are also included in its
 * hash computation.  This means we can only support containment queries,
 * but the index can distinguish, for example, {"foo": 42} from {"bar": 42}
 * since different hashes will be generated.
 *
 */

PG_FUNCTION_INFO_V1(gin_extract_vpack_path);

Datum
gin_extract_vpack_path(PG_FUNCTION_ARGS)
{
	Vpack	   *vpack = PG_GETARG_VPACK(0);
	VpackSlice	slice = vpack_to_slice(vpack);

	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	int			total = slice.isObject() || slice.isArray() ? 2 * slice.length() : 1;

	PathHashStack tail;
	PathHashStack *stack;
	int			i = 0;
	Datum	   *entries;

	/* If the root level is empty, we certainly have no keys */
	if (total == 0)
	{
		*nentries = 0;
		PG_RETURN_POINTER(NULL);
	}

	/* Otherwise, use 2 * root count as initial estimate of result size */
	entries = (Datum *) palloc(sizeof(Datum) * total);

	/* We keep a stack of partial hashes corresponding to parent key levels */
	tail.parent = NULL;
	tail.hash = 0;
	stack = &tail;

	gin_extract_vpack_path_append(slice, &i, &entries, &total, &stack);

	*nentries = i;

	PG_RETURN_POINTER(entries);
}

void gin_extract_vpack_path_append_value(const VpackSlice& it, int *i, Datum **entries, int *total, PathHashStack **stack)
{
	if(it.isArray() || it.isObject())
	{
		gin_extract_vpack_path_append(it, i, entries, total, stack);
	}
	else
	{
		gin_extract_vpack_ensure_size(i, entries, total);

		VpackHashScalarValue(it, &(*stack)->hash);

		(*entries)[(*i)++] = UInt32GetDatum((*stack)->hash);

		(*stack)->hash = (*stack)->parent->hash;
	}
}

void
gin_extract_vpack_path_append(const VpackSlice& slice, int *i, Datum **entries, int *total, PathHashStack **stack)
{
	PathHashStack *parent;

	if(slice.isObject())
	{
		for(auto const& it : VpackObjectIterator(slice))
		{
			parent = *stack;
			*stack = (PathHashStack *) palloc(sizeof(PathHashStack));
			(*stack)->hash = parent->hash;
			(*stack)->parent = parent;

			VpackHashScalarValue(it.key, &(*stack)->hash);

			gin_extract_vpack_path_append_value(it.value, i, entries, total, stack);

			parent = (*stack)->parent;
			pfree(*stack);
			*stack = parent;
			/* reset hash for next key, value, or sub-object */
			if ((*stack)->parent)
				(*stack)->hash = (*stack)->parent->hash;
			else
				(*stack)->hash = 0;
		}
	}
	else if(slice.isArray())
	{
		int k = 0;
		for(auto const& it : VpackArrayIterator(slice))
		{
			parent = *stack;
			*stack = (PathHashStack *) palloc(sizeof(PathHashStack));
			(*stack)->hash = parent->hash;
			(*stack)->parent = parent;

			gin_extract_vpack_path_append_value(it, i, entries, total, stack);

			parent = (*stack)->parent;
			pfree(*stack);
			*stack = parent;
			/* reset hash for next key, value, or sub-object */
			if ((*stack)->parent)
				(*stack)->hash = (*stack)->parent->hash;
			else
				(*stack)->hash = 0;

			k++;
		}
	}
	else
	{
		gin_extract_vpack_path_append_value(slice, i, entries, total, stack);
	}
}


PG_FUNCTION_INFO_V1(gin_extract_vpack_query_path);

Datum
gin_extract_vpack_query_path(PG_FUNCTION_ARGS)
{
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	int32	   *searchMode = (int32 *) PG_GETARG_POINTER(6);
	Datum	   *entries;

	if (strategy != VpackContainsStrategyNumber)
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	/* Query is a vpack, so just apply gin_extract_vpack_path ... */
	entries = (Datum *)
		DatumGetPointer(DirectFunctionCall2(gin_extract_vpack_path,
											PG_GETARG_DATUM(0),
											PointerGetDatum(nentries)));

	/* ... although "contains {}" requires a full index scan */
	if (*nentries == 0)
		*searchMode = GIN_SEARCH_MODE_ALL;

	PG_RETURN_POINTER(entries);
}


PG_FUNCTION_INFO_V1(gin_consistent_vpack_path);

Datum
gin_consistent_vpack_path(PG_FUNCTION_ARGS)
{
	bool	   *check = (bool *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);
	bool		res = true;
	int32		i;

	if (strategy != VpackContainsStrategyNumber)
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	/*
	 * vpack_path_ops is necessarily lossy, not only because of hash
	 * collisions but also because it doesn't preserve complete information
	 * about the structure of the JSON object.  Besides, there are some
	 * special rules around the containment of raw scalars in arrays that are
	 * not handled here.  So we must always recheck a match.  However, if not
	 * all of the keys are present, the tuple certainly doesn't match.
	 */
	*recheck = true;
	for (i = 0; i < nkeys; i++)
	{
		if (!check[i])
		{
			res = false;
			break;
		}
	}

	PG_RETURN_BOOL(res);
}


PG_FUNCTION_INFO_V1(gin_triconsistent_vpack_path);

Datum
gin_triconsistent_vpack_path(PG_FUNCTION_ARGS)
{
	GinTernaryValue *check = (GinTernaryValue *) PG_GETARG_POINTER(0);
	StrategyNumber strategy = PG_GETARG_UINT16(1);

	/* Jsonb	   *query = PG_GETARG_JSONB_P(2); */
	int32		nkeys = PG_GETARG_INT32(3);

	/* Pointer	   *extra_data = (Pointer *) PG_GETARG_POINTER(4); */
	GinTernaryValue res = GIN_MAYBE;
	int32		i;

	if (strategy != VpackContainsStrategyNumber)
		elog(ERROR, "unrecognized strategy number: %d", strategy);

	/*
	 * Note that we never return GIN_TRUE, only GIN_MAYBE or GIN_FALSE; this
	 * corresponds to always forcing recheck in the regular consistent
	 * function, for the reasons listed there.
	 */
	for (i = 0; i < nkeys; i++)
	{
		if (check[i] == GIN_FALSE)
		{
			res = GIN_FALSE;
			break;
		}
	}

	PG_RETURN_GIN_TERNARY_VALUE(res);
}

/*
 * Construct a vpack_ops GIN key from a flag byte and a textual representation
 * (which need not be null-terminated).  This function is responsible
 * for hashing overlength text representations; it will add the
 * VGINFLAG_HASHED bit to the flag value if it does that.
 */
static Datum
make_text_key(char flag, const char *str, int len)
{
	text	   *item;
	char		hashbuf[10];

	if (len > VGIN_MAXLENGTH)
	{
		uint32		hashval;

		hashval = DatumGetUInt32(hash_any((const unsigned char *) str, len));
		snprintf(hashbuf, sizeof(hashbuf), "%08x", hashval);
		str = hashbuf;
		len = 8;
		flag |= VGINFLAG_HASHED;
	}

	/*
	 * Now build the text Datum.  For simplicity we build a 4-byte-header
	 * varlena text Datum here, but we expect it will get converted to short
	 * header format when stored in the index.
	 */
	item = (text *) palloc(VARHDRSZ + len + 1);
	SET_VARSIZE(item, VARHDRSZ + len + 1);

	*VARDATA(item) = flag;

	memcpy(VARDATA(item) + 1, str, len);

	return PointerGetDatum(item);
}

/*
 * Create a textual representation of a JsonbValue that will serve as a GIN
 * key in a vpack_ops index.  is_key is true if the JsonbValue is a key,
 * or if it is a string array element (since we pretend those are keys,
 * see vpack.h).
 */
static Datum
make_scalar_key(const VpackSlice& scalarVal, bool is_key)
{
	Datum		item;
	char 	   *str;

	switch (scalarVal.type())
	{
		case VpackValueType::Array:
		case VpackValueType::Object:
			elog(ERROR, "unrecognized vpack scalar type: %d", scalarVal.type());
			break;
		case VpackValueType::None:
		case VpackValueType::Illegal:
		case VpackValueType::Null:
			Assert(!is_key);
			item = make_text_key(VGINFLAG_NULL, "", 0);
			break;
		case VpackValueType::Bool:
			Assert(!is_key);
			item = make_text_key(VGINFLAG_BOOL,
								 scalarVal.getBoolean() ? "t" : "f", 1);
			break;
		case VpackValueType::String:
			VpackValueLength length;
			str = (char*) scalarVal.getString(length);
			item = make_text_key(is_key ? VGINFLAG_KEY : VGINFLAG_STR,
								 str,
								 length);
			break;
		default:
			Assert(!is_key);

			str = slice_to_cstring(scalarVal);

			item = make_text_key(VGINFLAG_STR,
								 str,
								 strlen(str));
			pfree(str);
			break;
	}

	return item;
}

void
VpackHashScalarValue(const VpackSlice& scalarVal, uint32 *hash)
{
	uint32		tmp;
	char	   *str;

	/* Compute hash value for scalarVal */
	switch (scalarVal.type())
	{
		case VpackValueType::None:
		case VpackValueType::Illegal:
		case VpackValueType::Null:
			tmp = 0x01;
			break;
		case VpackValueType::Int:
		case VpackValueType::UInt:
		case VpackValueType::SmallInt:
			tmp = DatumGetUInt32(DirectFunctionCall1(hashint8, Int64GetDatum(scalarVal.getInt())));
			break;
		case VpackValueType::String:
			VpackValueLength length;
			str = (char*) scalarVal.getString(length);
			tmp = DatumGetUInt32(hash_any((const unsigned char *) str, length));
			break;
		case VpackValueType::Bool:
			tmp = scalarVal.getBoolean() ? 0x02 : 0x04;
			break;
		default:
			str = slice_to_cstring(scalarVal);
			tmp = DatumGetUInt32(hash_any((const unsigned char *) str, strlen(str)));
			pfree(str);
			break;
	}

	/*
	 * Combine hash values of successive keys, values and elements by rotating
	 * the previous value left 1 bit, then XOR'ing in the new
	 * key/value/element's hash value.
	 */
	*hash = (*hash << 1) | (*hash >> 31);
	*hash ^= tmp;
}


} // extern "C"
