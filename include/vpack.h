
#ifndef VPACK_H
#define VPACK_H

extern "C" {
#include "postgres.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "utils/jsonb.h"
#include "catalog/namespace.h"
#include "nodes/pathnodes.h"
#include "utils/selfuncs.h"
}

#include <limits.h>
#include <execinfo.h>
#include <iostream>

#include "velocypack/vpack.h"
#include "velocypack/Compare.h"

#include "memory.h"


using VpackValueLength = arangodb::velocypack::ValueLength;
using VpackArrayIterator = arangodb::velocypack::ArrayIterator;
using VpackObjectIterator = arangodb::velocypack::ObjectIterator;
using VpackBuilder = arangodb::velocypack::Builder;
using VpackObjectBuilder = arangodb::velocypack::ObjectBuilder;
using VpackArrayBuilder = arangodb::velocypack::ArrayBuilder;
using VpackBuilderNonDeleter = arangodb::velocypack::BuilderNonDeleter;
using VpackBuilderContainer = arangodb::velocypack::BuilderContainer;
using VpackCharBuffer = arangodb::velocypack::CharBuffer;
template<typename T> using VpackBuffer = arangodb::velocypack::Buffer<T>;
using VpackSink = arangodb::velocypack::Sink;
using VpackCharBufferSink = arangodb::velocypack::CharBufferSink;
using VpackStringSink = arangodb::velocypack::StringSink;
using VpackStringStreamSink = arangodb::velocypack::StringStreamSink;
using VpackCollection = arangodb::velocypack::Collection;
using VpackAttributeTranslator = arangodb::velocypack::AttributeTranslator;
using VpackDumper = arangodb::velocypack::Dumper;
using VpackException = arangodb::velocypack::Exception;
using VpackHexDump = arangodb::velocypack::HexDump;
using VpackOptions = arangodb::velocypack::Options;
using VpackCustomTypeHandler = arangodb::velocypack::CustomTypeHandler;
using VpackParser = arangodb::velocypack::Parser;
using VpackSlice = arangodb::velocypack::Slice;
using VpackSliceContainer = arangodb::velocypack::SliceContainer;
using VpackStringRef = arangodb::velocypack::StringRef;
using VpackUtf8Helper = arangodb::velocypack::Utf8Helper;
using VpackValidator = arangodb::velocypack::Validator;
using VpackValue = arangodb::velocypack::Value;
using VpackValuePair = arangodb::velocypack::ValuePair;
using VpackValueType = arangodb::velocypack::ValueType;
using VpackVersion = arangodb::velocypack::Version;
using VpackNormalizedCompare = arangodb::velocypack::NormalizedCompare;


extern "C" {

/*
 * Debug logging macros
 */
#ifdef VPACKDEBUG
#define VPACK1_elog(a,b)				elog(a,b)
#define VPACK2_elog(a,b,c)				elog(a,b,c)
#define VPACK3_elog(a,b,c,d)			elog(a,b,c,d)
#define VPACK4_elog(a,b,c,d,e)			elog(a,b,c,d,e)
#define VPACK5_elog(a,b,c,d,e,f)		elog(a,b,c,d,e,f)
#define VPACK6_elog(a,b,c,d,e,f,g)		elog(a,b,c,d,e,f,g)
#else
#define VPACK1_elog(a,b)
#define VPACK2_elog(a,b,c)
#define VPACK3_elog(a,b,c,d)
#define VPACK4_elog(a,b,c,d,e)
#define VPACK5_elog(a,b,c,d,e,f)
#define VPACK6_elog(a,b,c,d,e,f,g)
#endif

/*
 * Global variables
 */
#ifdef VPACK_CPP
Oid VPACK_OID;
VpackOptions dumperOptions;
#else
extern Oid VPACK_OID;
extern VpackOptions dumperOptions;
#endif


/*
 * C++ uncaught exception handler
 */
void inline
VpackUncaught()
{
	const char *message;

    try
    {
    	throw;
    }
    catch(const std::exception& e)
    {
    	message = e.what();
    }
    catch(...) {}

#ifdef DEBUG
    std::cout << "Uncaught exception: " << message << "\n";

    void *trace_elems[20];
    int trace_elem_count(backtrace(trace_elems, 20));
    char **stack_syms(backtrace_symbols(trace_elems, trace_elem_count));
    for(int i = 0; i < trace_elem_count; ++i)
    {
        std::cout << stack_syms[i] << "\n";
    }
    free( stack_syms );
#endif

    /* Context callbacks might not work right if call stack has been unwound */
    error_context_stack = NULL;

	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("unhandled C++ exception"),
			 errdetail("%s", message)));
}


/*
 * Vpack type struct and relevant macros
 */
typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	char        data;
} Vpack;

#define DatumGetVpack(d)	((Vpack *) PG_DETOAST_DATUM(d))
#define VpackGetDatum(d)	PointerGetDatum(d)
#define DatumGetVpackCopy(d) ((Vpack *) PG_DETOAST_DATUM_COPY(d))
#define PG_GETARG_VPACK(x)	DatumGetVpack(PG_GETARG_DATUM(x))
#define PG_RETURN_VPACK(x)	PG_RETURN_POINTER(x)
#define PG_GETARG_VPACK_COPY(x)	DatumGetVpackCopy(PG_GETARG_DATUM(x))

/*
 * Tag IDs -- will change
 */
#define VPACK_PG_NUMERIC 1
#define VPACK_PG_DATE 2
#define VPACK_PG_TIME 3
#define VPACK_PG_TIMETZ 4
#define VPACK_PG_TIMESTAMP 5
#define VPACK_PG_TIMESTAMPTZ 6
#define VPACK_PG_INTERVAL 7
#define VPACK_PG_BYTE 8
#define VPACK_PG_CHAR 9
#define VPACK_PG_NAME 10
#define VPACK_PG_JSON 11
#define VPACK_PG_UNKNOWN 12

#define TAG_OBJECTID 55
#define TAG_REGEX 56
#define TAG_COLLECTION 57
#define TAG_CODE 58
#define TAG_SYMBOL 59
#define TAG_CODESCOPE 60
#define TAG_DECIMAL 61

void composite_to_vpack(char *name, Datum composite, VpackBuilder *b);
void array_to_vpack(char *name, Datum array, VpackBuilder *b);
void array_dim_to_vpack(char* name, VpackBuilder *builder, int dim, int ndims, int *dims, Datum *vals, bool *nulls, int *valcount, Oid element_type);
void datum_to_vpack(char *name, Datum val, Oid typoid, VpackBuilder *b, bool isnull);


/*
 * Utility functions
 */

Vpack inline*
copy_to_vpack(char *src, size_t len)
{
	Vpack	   *out;

	out = (Vpack *) palloc(VARHDRSZ + len);
	SET_VARSIZE(out, VARHDRSZ + len);
	memcpy(VARDATA(out), src, len);

	return out;
}

Vpack inline*
buffer_to_vpack(VpackBuffer<uint8_t> *b)
{
	return copy_to_vpack((char*) b->data(), b->size());
}

Vpack inline*
slice_to_vpack(const VpackSlice &s)
{
	return copy_to_vpack((char*) s.start(), s.byteSize());
}

Vpack inline*
slice_p_to_vpack(VpackSlice *p)
{
	VpackSlice s = *p;
	return copy_to_vpack((char*) s.start(), s.byteSize());
}

Vpack inline*
builder_to_vpack(VpackBuilder *b)
{
	VpackBuffer<uint8_t> *buffer = b->buffer().get();

	return buffer_to_vpack(buffer);
}

Vpack inline*
empty_vpack()
{
	Vpack	   *out;

	out = (Vpack *) palloc(VARHDRSZ);
	SET_VARSIZE(out, VARHDRSZ);

	return out;
}

VpackBuffer<uint8_t> inline*
make_buffer(char *data, uint64_t len)
{
	VpackBuffer<uint8_t> *buffer = new VpackBuffer<uint8_t>();
	buffer->append(data, len);
	return buffer;
}

VpackBuilder inline*
make_builder(char *data, uint64_t len)
{
	VpackBuffer<uint8_t> *buffer = make_buffer(data, len);
	return new VpackBuilder(*buffer);
}

VpackBuilder inline*
vpack_to_builder(Vpack *value)
{
	return make_builder(&value->data, VARSIZE(value)-VARHDRSZ);
}

VpackSlice inline
vpack_to_slice(Vpack *value)
{
	return VpackSlice((const uint8_t*)(&(value->data)));
}

VpackSlice inline*
vpack_to_slice_p(Vpack *value)
{
	return new VpackSlice((const uint8_t*)(&(value->data)));
}

VpackBuilder inline*
jsonb_to_builder(Jsonb *json)
{
	// TODO: direct iteration

	JsonbContainer *jsonc = (JsonbContainer *) VARDATA(json);
	char	   *text = JsonbToCString(nullptr, jsonc, VARSIZE(json)-VARHDRSZ);

	VpackBuilder *b = new VpackBuilder();

	VpackParser	parser(*b);
	parser.parse(text, strlen(text));

	return b;
}

VpackBuilder inline*
json_to_builder(char *text, size_t len)
{
	VpackBuilder *b = new VpackBuilder();

	VpackParser	parser(*b);
	parser.parse(text, len);

	return b;
}


std::string inline*
slice_to_string(const VpackSlice &s)
{
	std::string *buffer = new std::string();

	VpackStringSink sink(buffer);

	if(s.isBinary() || s.isCustom())
	{
		sink.push_back('"');
		sink.append(s.toHex());
		sink.push_back('"');
	}
	else
	{
		VpackDumper dumper(&sink, &dumperOptions);
		dumper.dump(s);
	}

	return buffer;
}


char inline*
slice_to_cstring(const VpackSlice &s)
{
	std::string *slice = slice_to_string(s);

	return pstrdup(slice->c_str()); // needs to be copied or pfree on it will fail
}


text inline*
slice_to_text(const VpackSlice &s)
{
	if(s.isString())
	{
		VpackValueLength length;
		const char *str = s.getString(length);

		return cstring_to_text_with_len(str, length);
	}

	std::string *buffer = slice_to_string(s);

	text	   *result = (text *) palloc(buffer->length() + VARHDRSZ);

	SET_VARSIZE(result, buffer->length() + VARHDRSZ);
	memcpy(VARDATA(result), buffer->c_str(), buffer->length());

	return result;
}


std::string inline*
vpack_to_string(Vpack *vpack)
{
	if(VARSIZE(vpack) - VARHDRSZ > 0)
	{
		VpackSlice s = vpack_to_slice(vpack);

		return slice_to_string(s);
	}

	return new std::string();
}


char inline*
vpack_to_cstring(Vpack *vpack)
{
	VpackSlice s = vpack_to_slice(vpack);

	return slice_to_cstring(s);
}


text inline*
vpack_to_text(Vpack *vpack)
{
	VpackSlice s = vpack_to_slice(vpack);

	return slice_to_text(s);
}


text inline*
string_to_text(std::string &s)
{
	text	   *result = (text *) palloc(s.length() + VARHDRSZ);

	SET_VARSIZE(result, s.length() + VARHDRSZ);
	memcpy(VARDATA(result), s.c_str(), s.length());

	return result;
}


} // extern "C"


/*
 * C++ exception macros
 */

#define VPACK_TRY                                                  \
  try {

#define VPACK_PG_CATCH                                             \
  } catch (std::exception const& ex) {                             \
		ereport(ERROR,                                             \
				(errcode(ERRCODE_INTERNAL_ERROR),                  \
				 errmsg("Vpack error"),                            \
				 errdetail("%s", ex.what())));                     \
		PG_RETURN_NULL();                                          \
  } catch (...) {                                                  \
		ereport(ERROR,                                             \
				(errcode(ERRCODE_INTERNAL_ERROR),                  \
				 errmsg("unknown Vpack error"),                    \
				 errdetail("Unknown Vpack error.")));               \
		PG_RETURN_NULL();                                          \
  }

#endif // VPACK_H
