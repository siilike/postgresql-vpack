#define VPACK_CPP

#include "vpack.h"

extern "C" {


#include "miscadmin.h"
#include "utils/numeric.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/jsonb.h"
#include "catalog/namespace.h"
#include "catalog/pg_operator.h"

PG_MODULE_MAGIC;


/*
 * TODO: make public in core
 */
static Oid
OperatorGet(const char *operatorName,
			Oid operatorNamespace,
			Oid leftObjectId,
			Oid rightObjectId,
			bool *defined);


/*
 * Extension initialization
 */
void _PG_init(void)
{
	std::set_terminate(VpackUncaught);
	setbuf(stdout, NULL);

	VPACK_OID = TypenameGetTypid("vpack");

	dumperOptions.singleLinePrettyPrint = true;
	dumperOptions.unsupportedDoublesAsString = true;
	dumperOptions.unsupportedTypeBehavior = VpackOptions::ConvertUnsupportedType;
	dumperOptions.binaryAsHex = true;
	dumperOptions.datesAsIntegers = true;

#ifdef DEBUG
	dumperOptions.debugTags = true;
#endif
}

void _PG_fini(void)
{
	// TOOD
}


PG_FUNCTION_INFO_V1(vpack_mirror);

Datum
vpack_mirror(PG_FUNCTION_ARGS)
{
	Vpack      *value = PG_GETARG_VPACK(0);
//	Vpack	   *ret;
//
//	ret = (Vpack *) palloc(VARSIZE(value));
//	SET_VARSIZE(ret, VARSIZE(value));
//	memcpy(VARDATA(ret), VARDATA(value), VARSIZE(value) - VARHDRSZ);

	VpackValidator v;

	try
	{
		Assert(v.validate(VARDATA(value), VARSIZE(value), false));
		PG_RETURN_VPACK(value);
	}
	catch(std::exception const& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("error parsing input"),
				 errdetail("%s", ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("error parsing input"),
				 errdetail("Unknown Vpack error.")));
	}

	PG_RETURN_NULL();
}


PG_FUNCTION_INFO_V1(vpack_in);

Datum
vpack_in(PG_FUNCTION_ARGS)
{
	char	   *json = PG_GETARG_CSTRING(0);

	if(strlen(json) == 0)
	{
		PG_RETURN_VPACK(empty_vpack());
	}

	try
	{
		VpackBuilder *b = json_to_builder(json, strlen(json));

		PG_RETURN_VPACK(builder_to_vpack(b));
	}
	catch (std::exception const& ex)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("error parsing input"),
				 errdetail("%s", ex.what())));
	}
	catch (...)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("error parsing input"),
				 errdetail("Unknown Vpack error.")));
	}
}


PG_FUNCTION_INFO_V1(vpack_out);

Datum
vpack_out(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack      *value = PG_GETARG_VPACK(0);

	if(VARSIZE(value) - VARHDRSZ == 0)
	{
		PG_RETURN_CSTRING("");
	}

	PG_RETURN_CSTRING(vpack_to_cstring(value));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(vpack_cast_text);

Datum
vpack_cast_text(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Vpack      *value = PG_GETARG_VPACK(0);

	PG_RETURN_TEXT_P(vpack_to_text(value));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(text_cast_vpack);

Datum
text_cast_vpack(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	text	   *key = PG_GETARG_TEXT_PP(0);

	VpackBuilder *b = json_to_builder(VARDATA_ANY(key), VARSIZE_ANY_EXHDR(key));

	PG_RETURN_VPACK(builder_to_vpack(b));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(jsonb_cast_vpack);

Datum
jsonb_cast_vpack(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Jsonb	   *jsonb = PG_GETARG_JSONB_P(0);

	VpackBuilder *b = jsonb_to_builder(jsonb);

	PG_RETURN_VPACK(builder_to_vpack(b));

	VPACK_PG_CATCH
}


PG_FUNCTION_INFO_V1(to_vpack);

Datum
to_vpack(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	Datum		val = PG_GETARG_DATUM(0);
	Oid			val_type = get_fn_expr_argtype(fcinfo->flinfo, 0);

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	VpackBuilder *builder = new VpackBuilder();

	datum_to_vpack(nullptr, val, val_type, builder, false);

	if(builder->isEmpty())
	{
		PG_RETURN_VPACK(empty_vpack());
	}

	PG_RETURN_VPACK(builder_to_vpack(builder));

	VPACK_PG_CATCH
}


void
vpack_builder_add_slice(VpackBuilder *b, char *name, uint64_t tag, const VpackSlice& value)
{
	if(name != nullptr)
	{
		b->addTagged(name, tag, value);
	}
	else
	{
		b->addTagged(tag, value);
	}
}

void
vpack_builder_add_value(VpackBuilder *b, char *name, uint64_t tag, const VpackValue& value)
{
	if(name != nullptr)
	{
		b->addTagged(name, tag, value);
	}
	else
	{
		b->addTagged(tag, value);
	}
}

void
vpack_builder_add_value_pair(VpackBuilder *b, char *name, uint64_t tag, const VpackValuePair& value)
{
	if(name != nullptr)
	{
		b->addTagged(name, tag, value);
	}
	else
	{
		b->addTagged(tag, value);
	}
}


void
composite_to_vpack(char *name, Datum composite, VpackBuilder *b)
{
	HeapTupleHeader td;
	Oid			tupType;
	int32		tupTypmod;
	TupleDesc	tupdesc;
	HeapTupleData tmptup,
			   *tuple;
	int			i;

	td = DatumGetHeapTupleHeader(composite);

	/* Extract rowtype info and find a tupdesc */
	tupType = HeapTupleHeaderGetTypeId(td);
	tupTypmod = HeapTupleHeaderGetTypMod(td);
	tupdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

	/* Build a temporary HeapTuple control structure */
	tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
	tmptup.t_data = td;
	tuple = &tmptup;

	vpack_builder_add_value(b, name, 0, VpackValue(VpackValueType::Object));

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		val;
		bool		isnull;
		char	   *attname;
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		attname = NameStr(att->attname);

		val = heap_getattr(tuple, i + 1, tupdesc, &isnull);

		datum_to_vpack(attname, val, att->atttypid, b, isnull);
	}

	b->close();

	ReleaseTupleDesc(tupdesc);
}


void
array_to_vpack(char *name, Datum array, VpackBuilder *b)
{
	ArrayType  *v = DatumGetArrayTypeP(array);
	Oid			element_type = ARR_ELEMTYPE(v);
	int		   *dim;
	int			ndim;
	int			nitems;
	int			count = 0;
	Datum	   *elements;
	bool	   *nulls;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	ndim = ARR_NDIM(v);
	dim = ARR_DIMS(v);
	nitems = ArrayGetNItems(ndim, dim);

	if(nitems <= 0)
	{
		vpack_builder_add_value(b, name, 0, VpackValue(VpackValueType::Array));
		b->close();
		return;
	}

	get_typlenbyvalalign(element_type,
						 &typlen, &typbyval, &typalign);

	deconstruct_array(v, element_type, typlen, typbyval,
					  typalign, &elements, &nulls,
					  &nitems);

	array_dim_to_vpack(name, b, 0, ndim, dim, elements, nulls, &count, element_type);

	pfree(elements);
	pfree(nulls);
}


/*
 * Process a single dimension of an array.
 * If it's the innermost dimension, output the values, otherwise call
 * ourselves recursively to process the next dimension.
 */
void
array_dim_to_vpack(char* name, VpackBuilder *builder, int dim, int ndims, int *dims,
				   Datum *vals, bool *nulls, int *valcount, Oid element_type)
{
	int			i;

	Assert(dim < ndims);

	builder->add(name, VpackValue(VpackValueType::Array));

	for (i = 1; i <= dims[dim]; i++)
	{
		if (dim + 1 == ndims)
		{
			datum_to_vpack(nullptr, vals[*valcount], element_type, builder, nulls[*valcount]);
			(*valcount)++;
		}
		else
		{
			array_dim_to_vpack(nullptr, builder, dim + 1, ndims, dims, vals, nulls,
							   valcount, element_type);
		}
	}

	builder->close();
}

void
datum_to_vpack(char *name, Datum val, Oid typoid, VpackBuilder *b, bool isnull)
{
	check_stack_depth();

	if(isnull)
	{
		vpack_builder_add_slice(b, name, 0, VpackSlice::nullSlice());
	}
	else
	{
		switch(typoid)
		{
			case BOOLOID:
				vpack_builder_add_slice(b, name, 0, DatumGetBool(val) ? VpackSlice::trueSlice() : VpackSlice::falseSlice());
				break;
	        case FLOAT4OID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(static_cast<float_t>(DatumGetFloat4(val))));
	            break;
	        case FLOAT8OID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(static_cast<double_t>(DatumGetFloat8(val))));
	            break;
	        case INT2OID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(static_cast<uint16_t>(DatumGetInt16(val))));
	            break;
	        case INT4OID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(static_cast<uint32_t>(DatumGetInt32(val))));
	            break;
	        case INT8OID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(static_cast<uint64_t>(DatumGetInt64(val))));
	            break;
	        case NUMERICOID:
				{
					Numeric 	n = DatumGetNumeric(val);
					char	   *t = numeric_normalize(n);

					vpack_builder_add_value(b, name,  VPACK_PG_NUMERIC, VpackValue(TextDatumGetCString(val)));
				}
				break;
	        case TIMESTAMPOID:
	        	{
	        		Timestamp	timestamp = DatumGetTimestamp(val);

	        	    if(TIMESTAMP_NOT_FINITE(timestamp))
	        	    {
	        	    	vpack_builder_add_slice(b, name,  VPACK_PG_TIMESTAMP, VpackSlice::nullSlice());
	        	    }
	        	    else
	        	    {
	        	        Timestamp	epoch = SetEpochTimestamp();
	        	        int64		result = timestamp - epoch;

	                    vpack_builder_add_value(b, name,  VPACK_PG_TIMESTAMP, VpackValue(result, VpackValueType::UTCDate));
	        	    }
	        	}
	            break;
	        case DATEOID:
	        	{
	        		DateADT		date = DatumGetDateADT(val);

	        		char	   *result;
	        		struct pg_tm tt,
	        				   *tm = &tt;
	        		char		buf[MAXDATELEN + 1];

	        		if (DATE_NOT_FINITE(date))
	        			EncodeSpecialDate(date, buf);
	        		else
	        		{
	        			j2date(date + POSTGRES_EPOCH_JDATE,
	        				   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
	        			EncodeDateOnly(tm, DateStyle, buf);
	        		}

	        		result = pstrdup(buf);

                    vpack_builder_add_value(b, name,  VPACK_PG_DATE, VpackValue(result));
	        	}
	            break;
// TODO
//	        case TIMEOID:
//	            DatumGetTimeADT
//	            break;
//	        case TIMETZOID:
//	            DatumGetTimeTzADTP
//	            break;
//	        case TIMESTAMPTZOID:
//	            DatumGetTimestampTz
//	            break;
//	        case INTERVALOID:
//	            DatumGetIntervalP
//	            break;
	        case BYTEAOID:
				{
					bytea 	   *bytes = DatumGetByteaPP(val);

					vpack_builder_add_value_pair(b, name, 0, VpackValuePair(VARDATA(bytes), VARSIZE(bytes) - VARHDRSZ, VpackValueType::Binary));
				}
	            break;
	        case CHAROID:
				vpack_builder_add_value(b, name,  VPACK_PG_CHAR, VpackValue((char) DatumGetChar(val)));
	            break;
	        case NAMEOID:
				vpack_builder_add_value(b, name,  VPACK_PG_NAME, VpackValue((char*) NameStr(*DatumGetName(val))));
	            break;
	        case JSONBOID:
				{
					VpackBuilder *builder = jsonb_to_builder(DatumGetJsonbP(val));

					vpack_builder_add_slice(b, name, 0, VpackSlice(builder->start()));
				}
	            break;
	        case JSONOID:
	        	{
	        		char	   *json = TextDatumGetCString(val);

					VpackBuilder *builder = json_to_builder(json, strlen(json));

					vpack_builder_add_slice(b, name, 0, VpackSlice(builder->start()));
	        	}
	            break;
	        case TEXTOID:
	        case BPCHAROID:
	        case VARCHAROID:
	        	vpack_builder_add_value(b, name, 0, VpackValue(TextDatumGetCString(val)));
	            break;
	        default:
	        	{
	        		if (typoid == VPACK_OID)
	        		{
	        			vpack_builder_add_slice(b, name, 0, vpack_to_slice(DatumGetVpack(val)));
	        		}
	        		else if (OidIsValid(get_element_type(typoid)) || typoid == ANYARRAYOID
	    				|| typoid == RECORDARRAYOID)
	    			{
	    				array_to_vpack(name, val, b);
	    			}
	    			else if (type_is_rowtype(typoid))	/* includes RECORDOID */
	    			{
						composite_to_vpack(name, val, b);
	    			}
	    			else
	    			{
						Oid			output_func;
						bool 		is_varlena;

						getTypeOutputInfo(typoid, &output_func, &is_varlena);

						if (is_varlena)
						{
							val = PointerGetDatum(PG_DETOAST_DATUM(val));
						}

						char *ret = OidOutputFunctionCall(output_func, val);

						vpack_builder_add_value(b, name,  VPACK_PG_UNKNOWN, VpackValue(ret));
	    			}
	        	}
	            break;
		}
	}
}

/*
 * SQL function vpack_build_object(variadic "any")
 */
PG_FUNCTION_INFO_V1(vpack_build_object);

Datum
vpack_build_object(PG_FUNCTION_ARGS)
{
	int			nargs;
	int			i;
	Datum	   *args;
	bool	   *nulls;
	Oid		   *types;
	VpackBuilder b;

	/* build argument values to build the object */
	nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

	if (nargs < 0)
		PG_RETURN_NULL();

	if (nargs % 2 != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("argument list must have even number of elements"),
		/* translator: %s is a SQL function name */
				 errhint("The arguments of %s must consist of alternating keys and values.",
						 "vpack_build_object()")));

	b.openObject();

	for (i = 0; i < nargs; i += 2)
	{
		/* process key */
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("argument %d: key must not be null", i + 1)));

		Oid key_type = types[i];
		Oid val_type = types[i + 1];

		if (key_type != TEXTOID)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("all keys need to be of text type")));
		}

		if (val_type == InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not determine input data type")));

		char* key = text_to_cstring(DatumGetTextP(args[i]));

		datum_to_vpack(key, args[i + 1], types[i + 1], &b, nulls[i + 1]);
	}

	b.close();

	PG_RETURN_POINTER(slice_to_vpack(VpackSlice(b.start())));
}


/*
 * SQL function jsonb_build_array(variadic "any")
 */
PG_FUNCTION_INFO_V1(vpack_build_array);

Datum
vpack_build_array(PG_FUNCTION_ARGS)
{
	int			nargs;
	int			i;
	Datum	   *args;
	bool	   *nulls;
	Oid		   *types;
	VpackBuilder b;

	/* build argument values to build the array */
	nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);

	if (nargs < 0)
		PG_RETURN_NULL();

	b.openArray();

	for (i = 0; i < nargs; i++)
	{
		datum_to_vpack(nullptr, args[i], types[i], &b, nulls[i]);
	}

	b.close();

	PG_RETURN_POINTER(slice_to_vpack(VpackSlice(b.start())));
}



/*
 * TODO: make public in core
 */
static Oid
OperatorGet(const char *operatorName,
			Oid operatorNamespace,
			Oid leftObjectId,
			Oid rightObjectId,
			bool *defined)
{
	HeapTuple	tup;
	Oid			operatorObjectId;

	tup = SearchSysCache4(OPERNAMENSP,
						  PointerGetDatum(operatorName),
						  ObjectIdGetDatum(leftObjectId),
						  ObjectIdGetDatum(rightObjectId),
						  ObjectIdGetDatum(operatorNamespace));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_operator oprform = (Form_pg_operator) GETSTRUCT(tup);

		operatorObjectId = oprform->oid;
		*defined = RegProcedureIsValid(oprform->oprcode);
		ReleaseSysCache(tup);
	}
	else
	{
		operatorObjectId = InvalidOid;
		*defined = false;
	}

	return operatorObjectId;
}


} // extern "C"
