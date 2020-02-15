
#include "vpack.h"

extern "C" {

#include "postgres.h"
#include "utils/array.h"
#include "catalog/pg_type.h"


static bool vpack_contains_worker(VpackSlice &valSlice, VpackSlice &tmplSlice);
static int vpack_cmp_worker(VpackSlice &a, VpackSlice &b);
static Datum get_vpack_path_all(FunctionCallInfo fcinfo, int type);


void
vpack_set_any_build(Datum *key_datums, bool *key_nulls, int key_count, VpackSlice &slice, int i, VpackBuilder *b, Datum val, Oid val_type)
{
	if(i == key_count)
	{
		datum_to_vpack(nullptr, val, val_type, b, false);
		return;
	}

	if (key_nulls[i])
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("path value is null")));
	}

	if(slice.isArray())
	{
		int idx = std::stoi(std::string(VARDATA(key_datums[i]), VARSIZE(key_datums[i]) - VARHDRSZ), nullptr, 10);

		b->openArray();

		int j = 0;
		for(auto const& it : VpackArrayIterator(slice))
		{
			if(j == idx)
			{
				slice = it;

				vpack_set_any_build(key_datums, key_nulls, key_count, slice, i+1, b, val, val_type);
			}
			else
			{
				b->add(it);
			}

			j++;
		}

		b->close();
	}
	else if(slice.isObject() || slice.isNone())
	{
		VpackStringRef k = VpackStringRef(VARDATA(key_datums[i]), VARSIZE(key_datums[i]) - VARHDRSZ);

		b->openObject();

		bool hasKey = false;

		if(slice.isObject())
		{
			for(auto const& it : VpackObjectIterator(slice))
			{
				if(it.key.stringRef().equals(k))
				{
					hasKey = true;

					b->add(it.key);

					slice = it.value;

					if(slice.isNone())
					{
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("path not found")));
					}

					vpack_set_any_build(key_datums, key_nulls, key_count, slice, i+1, b, val, val_type);
				}
				else
				{
					b->add(it.key.stringRef(), it.value);
				}
			}
		}

		if(!hasKey)
		{
			b->add(VpackValue(k.toString()));

			slice = VpackSlice::noneSlice();

			vpack_set_any_build(key_datums, key_nulls, key_count, slice, i+1, b, val, val_type);
		}

		b->close();
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid path element")));
	}
}


PG_FUNCTION_INFO_V1(vpack_set);

Datum
vpack_set(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *src = PG_GETARG_VPACK(0);
	ArrayType  *query = PG_GETARG_ARRAYTYPE_P(1);
	Datum		val = PG_GETARG_DATUM(2);
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	Datum	   *key_datums;
	bool	   *key_nulls;
	int			key_count;

	VpackSlice 	slice = vpack_to_slice(src);
	VpackBuilder *b = new VpackBuilder();

	deconstruct_array(query,
					  TEXTOID, -1, false, 'i',
					  &key_datums, &key_nulls, &key_count);

	vpack_set_any_build(key_datums, key_nulls, key_count, slice, 0, b, val, val_type);

	PG_RETURN_VPACK(builder_to_vpack(b));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_concat);

Datum
vpack_concat(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp1 = PG_GETARG_VPACK(0);
	Vpack	   *vp2 = PG_GETARG_VPACK(1);

	VpackSlice 	s1 = vpack_to_slice(vp1);
	VpackSlice 	s2 = vpack_to_slice(vp2);

	if(s1.isObject() && s2.isObject())
	{
		if(s1.isEmptyObject())
		{
			PG_RETURN_VPACK(slice_to_vpack(s2));
		}
		else if(s2.isEmptyObject())
		{
			PG_RETURN_VPACK(slice_to_vpack(s1));
		}

		VpackBuilder *b = new VpackBuilder();

		VpackCollection::merge(*b, s1, s2, true, false);

		PG_RETURN_VPACK(builder_to_vpack(b));
	}
	else if(s1.isArray() && s2.isArray())
	{
		if(s1.isEmptyArray())
		{
			PG_RETURN_VPACK(slice_to_vpack(s2));
		}
		else if(s2.isEmptyArray())
		{
			PG_RETURN_VPACK(slice_to_vpack(s1));
		}

		VpackBuilder *b = new VpackBuilder();

		b->openArray();
		VpackCollection::appendArray(*b, s1);
		VpackCollection::appendArray(*b, s2);
		b->close();

		PG_RETURN_VPACK(builder_to_vpack(b));
	}
	else if(s1.isArray())
	{
		VpackBuilder *b = new VpackBuilder();

		b->openArray();
		VpackCollection::appendArray(*b, s1);
		b->add(s2);
		b->close();

		PG_RETURN_VPACK(builder_to_vpack(b));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("one or more of the input types not supported for this operation")));
	}

	VPACK_PG_CATCH
}


/*
 * SELECT '[1,2,3]'::vpack || '5'; = [1,2,3,5]
 * SELECT '[1,2,"3"]'::vpack || '"5"'; = [1,2,"3","5"]
 */

PG_FUNCTION_INFO_V1(vpack_concat_any);

Datum
vpack_concat_any(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	Datum		val = PG_GETARG_DATUM(1);
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	VpackSlice 	slice = vpack_to_slice(vp);

	VpackBuilder *b = new VpackBuilder();

	if(slice.isArray())
	{
		b->openArray();
		VpackCollection::appendArray(*b, slice);
	}
	else if(slice.isObject())
	{
		b->openObject();

		VpackObjectIterator it(slice);
		while (it.valid())
		{
		    auto key = it.key(true).copyString();
			b->add(key, it.value());
			it.next();
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("one or more of the input types not supported for this operation")));
	}

	datum_to_vpack(nullptr, val, val_type, b, false);

	b->close();

	PG_RETURN_VPACK(builder_to_vpack(b));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_object_field);

Datum
vpack_object_field(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isObject())
	{
		r = s.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));
	}

	if(r.isNone())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field %s not found in object or value not an object", text_to_cstring(key))));

		PG_RETURN_NULL();
	}

	PG_RETURN_VPACK(slice_to_vpack(r));

	VPACK_PG_CATCH
}



PG_FUNCTION_INFO_V1(vpack_array_field);

Datum
vpack_array_field(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	int32		key = PG_GETARG_INT32(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isArray() && s.length())
	{
		try
		{
			r = s.at(key);
		}
		catch(...)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("item %d not found from array", key)));

			PG_RETURN_NULL();
		}
	}

	if(r.isNone())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("item %d not found from array or value not an array", key)));

		PG_RETURN_NULL();
	}

	PG_RETURN_VPACK(slice_to_vpack(r));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_object_field_string);

Datum
vpack_object_field_string(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isObject())
	{
		r = s.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));
	}

	if(r.isNone())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field %s not found in object or value not an object", text_to_cstring(key))));

		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(slice_to_text(r));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_array_field_string);

Datum
vpack_array_field_string(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	int32		key = PG_GETARG_INT32(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isArray() && s.length())
	{
		try
		{
			r = s.at(key);
		}
		catch(...)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("item %d not found from array", key)));

			PG_RETURN_NULL();
		}
	}

	if(r.isNone())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("item %d not found from array or value not an array", key)));

		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(slice_to_text(r));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_object_field_integer);

Datum
vpack_object_field_integer(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isObject())
	{
		r = s.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));
	}

	if(!r.isInteger())
	{
		ereport(NOTICE,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("field %s not found in object, input not an object or value not an integer", text_to_cstring(key))));

		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(r.getInt());

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_contains);

Datum
vpack_contains(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	Vpack	   *tmpl = PG_GETARG_VPACK(1);
	VpackSlice	valSlice = vpack_to_slice(val);
	VpackSlice	tmplSlice = vpack_to_slice(tmpl);

	PG_RETURN_BOOL(vpack_contains_worker(valSlice, tmplSlice));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_contained);

Datum
vpack_contained(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	Vpack	   *tmpl = PG_GETARG_VPACK(1);
	VpackSlice	valSlice = vpack_to_slice(val);
	VpackSlice	tmplSlice = vpack_to_slice(tmpl);

	PG_RETURN_BOOL(vpack_contains_worker(tmplSlice, valSlice));

	VPACK_PG_CATCH
}


static bool
vpack_contains_worker(VpackSlice &valSlice, VpackSlice &tmplSlice)
{
	if(tmplSlice.isObject())
	{
		if(!valSlice.isObject())
		{
			return false;
		}

		for(auto const& it : VpackObjectIterator(tmplSlice))
		{
			VpackSlice v = valSlice.get(it.key.stringRef());

			if(v.isNone())
			{
				return false;
			}

			VpackSlice s = it.value;

			if(!vpack_contains_worker(v, s))
			{
				return false;
			}
		}
	}
	else if(tmplSlice.isArray())
	{
		if(!valSlice.isArray())
		{
			return false;
		}

		for(auto const& it : VpackArrayIterator(tmplSlice))
		{
			bool		res = false;

			for(auto const& it2 : VpackArrayIterator(valSlice))
			{
				VpackSlice s = it;
				VpackSlice s2 = it2;

				if(vpack_contains_worker(s2, s))
				{
					res = true;
				}
			}

			if(!res)
			{
				return false;
			}
		}
	}
	else
	{
		if(!valSlice.binaryEquals(tmplSlice))
		{
			return false;
		}
	}

	return true;
}


PG_FUNCTION_INFO_V1(vpack_exists);

Datum
vpack_exists(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);
	VpackSlice	valSlice = vpack_to_slice(val);

	if(valSlice.isObject())
	{
		PG_RETURN_BOOL(valSlice.hasKey(text_to_cstring(key)));
	}

	PG_RETURN_BOOL(false);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_exists_any);

Datum
vpack_exists_any(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	int			i;
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			elem_count;
	VpackSlice	valSlice = vpack_to_slice(val);

	deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls,
					  &elem_count);

	for (i = 0; i < elem_count; i++)
	{
		if (key_nulls[i])
			continue;

		if(valSlice.hasKey(text_to_cstring((text*) key_datums[i])))
		{
			PG_RETURN_BOOL(true);
		}
	}

	PG_RETURN_BOOL(false);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_exists_all);

Datum
vpack_exists_all(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	ArrayType  *keys = PG_GETARG_ARRAYTYPE_P(1);
	int			i;
	Datum	   *key_datums;
	bool	   *key_nulls;
	int			elem_count;
	VpackSlice	valSlice = vpack_to_slice(val);

	deconstruct_array(keys, TEXTOID, -1, false, 'i', &key_datums, &key_nulls,
					  &elem_count);

	for (i = 0; i < elem_count; i++)
	{
		if (key_nulls[i])
			continue;

		if(!valSlice.hasKey(text_to_cstring((text*) key_datums[i])))
		{
			PG_RETURN_BOOL(false);
		}
	}

	PG_RETURN_BOOL(true);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_hash);

Datum
vpack_hash(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *val = PG_GETARG_VPACK(0);
	VpackSlice	valSlice = vpack_to_slice(val);
	uint32_t	hash = valSlice.hash32();

	PG_FREE_IF_COPY(val, 0);
	PG_RETURN_INT32(hash);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_cmp);

Datum
vpack_cmp(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	int			res;

	res = vpack_cmp_worker(aSlice, bSlice);

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_INT32(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_lt);

Datum
vpack_lt(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = vpack_cmp_worker(aSlice, bSlice) < 0;

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_lt_integer);

Datum
vpack_lt_integer(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	int64	   	b = PG_GETARG_INT64(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	bool		res = false;

	if(aSlice.isInteger())
	{
		res = aSlice.getInt() < b;
	}

	PG_FREE_IF_COPY(a, 0);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_gt);

Datum
vpack_gt(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = vpack_cmp_worker(aSlice, bSlice) > 0;

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_le);

Datum
vpack_le(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = vpack_cmp_worker(aSlice, bSlice) <= 0;

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_ge);

Datum
vpack_ge(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = vpack_cmp_worker(aSlice, bSlice) >= 0;

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


static int
vpack_cmp_worker(VpackSlice &a, VpackSlice &b)
{
	return a.byteSize() - b.byteSize();
}


PG_FUNCTION_INFO_V1(vpack_eq);

Datum
vpack_eq(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = aSlice.binaryEquals(bSlice);

	if(!res)
	{
		res = VpackNormalizedCompare::equals(aSlice, bSlice);
	}

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_ne);

Datum
vpack_ne(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *a = PG_GETARG_VPACK(0);
	Vpack	   *b = PG_GETARG_VPACK(1);
	VpackSlice	aSlice = vpack_to_slice(a);
	VpackSlice	bSlice = vpack_to_slice(b);
	bool		res;

	res = !aSlice.binaryEquals(bSlice);

	if(res)
	{
		res = !VpackNormalizedCompare::equals(aSlice, bSlice);
	}

	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(res);

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_extract_path);

Datum
vpack_extract_path(PG_FUNCTION_ARGS)
{
	return get_vpack_path_all(fcinfo, 0);
}


PG_FUNCTION_INFO_V1(vpack_extract_path_text);

Datum
vpack_extract_path_text(PG_FUNCTION_ARGS)
{
	return get_vpack_path_all(fcinfo, 1);
}


PG_FUNCTION_INFO_V1(vpack_extract_path_integer);

Datum
vpack_extract_path_integer(PG_FUNCTION_ARGS)
{
	return get_vpack_path_all(fcinfo, 2);
}


static Datum
get_vpack_path_all(FunctionCallInfo fcinfo, int type)
{
	Vpack	   *input = PG_GETARG_VPACK(0);
	ArrayType  *path = PG_GETARG_ARRAYTYPE_P(1);
	Datum	   *pathtext;
	bool	   *pathnulls;
	int			npath;
	int			i;

	/*
	 * If the array contains any null elements, return NULL, on the grounds
	 * that you'd have gotten NULL if any RHS value were NULL in a nested
	 * series of applications of the -> operator.  (Note: because we also
	 * return NULL for error cases such as no-such-field, this is true
	 * regardless of the contents of the rest of the array.)
	 */
	if (array_contains_nulls(path))
		PG_RETURN_NULL();

	deconstruct_array(path, TEXTOID, -1, false, 'i',
					  &pathtext, &pathnulls, &npath);

	VpackSlice	res = vpack_to_slice(input);

	for (i = 0; i < npath; i++)
	{
		char	   *indextext = TextDatumGetCString(pathtext[i]);

		if(res.isObject())
		{
			res = res.get(indextext);
		}
		else if(res.isArray())
		{
			res = res.at(std::atoi(indextext));
		}
		else
		{
			PG_RETURN_NULL();
		}
	}

	if(res.isNone() || res.isNull())
	{
		PG_RETURN_NULL();
	}

	if(type == 1)
	{
		PG_RETURN_TEXT_P(slice_to_text(res));
	}
	else if(type == 2)
	{
		if(!res.isInteger())
		{
			PG_RETURN_NULL();
		}

		PG_RETURN_INT64(res.getInt());
	}

	PG_RETURN_VPACK(slice_to_vpack(res));
}


PG_FUNCTION_INFO_V1(vpack_object_field_in_array);

Datum
vpack_object_field_in_array(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isArray())
	{
		for(auto const& it : VpackArrayIterator(s))
		{
			if(it.isObject())
			{
				r = it.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

				if(!r.isNone())
				{
					break;
				}
			}
		}
	}

	if(r.isNone())
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_VPACK(slice_to_vpack(r));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_object_field_string_in_array);

Datum
vpack_object_field_string_in_array(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isArray())
	{
		for(auto const& it : VpackArrayIterator(s))
		{
			if(it.isObject())
			{
				r = it.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

				if(!r.isNone())
				{
					break;
				}
			}
		}
	}

	if(r.isNone())
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_TEXT_P(slice_to_text(r));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_object_field_integer_in_array);

Datum
vpack_object_field_integer_in_array(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack	   *vp = PG_GETARG_VPACK(0);
	text	   *key = PG_GETARG_TEXT_PP(1);

	VpackSlice	s = vpack_to_slice(vp);
	VpackSlice  r;

	if(s.isArray())
	{
		for(auto const& it : VpackArrayIterator(s))
		{
			if(it.isObject())
			{
				r = it.get(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

				if(!r.isNone())
				{
					break;
				}
			}
		}
	}

	if(!r.isInteger())
	{
		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(r.getInt());

	VPACK_PG_CATCH
}






} // extern "C"
