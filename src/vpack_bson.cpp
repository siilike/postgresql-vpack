
#include "vpack.h"

extern "C" {

#include <bson/bson.h>

bool visit_before(const bson_iter_t *iter, const char *key, void *data);
bool visit_after(const bson_iter_t *iter, const char *key, void *data);
void visit_corrupt(const bson_iter_t *iter, void *data);
bool visit_double(const bson_iter_t *iter, const char *key, double v_double, void *data);
bool visit_utf8(const bson_iter_t *iter, const char *key, size_t v_utf8_len, const char *v_utf8, void *data);
bool visit_document(const bson_iter_t *iter, const char *key, const bson_t *v_document, void *data);
bool visit_array(const bson_iter_t *iter, const char *key, const bson_t *v_array, void *data);
bool visit_binary(const bson_iter_t *iter, const char *key, bson_subtype_t v_subtype, size_t v_binary_len, const uint8_t *v_binary, void *data);
bool visit_undefined(const bson_iter_t *iter, const char *key, void *data);
bool visit_oid(const bson_iter_t *iter, const char *key, const bson_oid_t *v_oid, void *data);
bool visit_bool(const bson_iter_t *iter, const char *key, bool v_bool, void *data);
bool visit_date_time(const bson_iter_t *iter, const char *key, int64_t msec_since_epoch, void *data);
bool visit_null(const bson_iter_t *iter, const char *key, void *data);
bool visit_regex(const bson_iter_t *iter, const char *key, const char *v_regex, const char *v_options, void *data);
bool visit_dbpointer(const bson_iter_t *iter, const char *key, size_t v_collection_len, const char *v_collection, const bson_oid_t *v_oid, void *data);
bool visit_code(const bson_iter_t *iter, const char *key, size_t v_code_len, const char *v_code, void *data);
bool visit_symbol(const bson_iter_t *iter, const char *key, size_t v_symbol_len, const char *v_symbol, void *data);
bool visit_codewscope(const bson_iter_t *iter, const char *key, size_t v_code_len, const char *v_code, const bson_t *v_scope, void *data);
bool visit_int32(const bson_iter_t *iter, const char *key, int32_t v_int32, void *data);
bool visit_timestamp(const bson_iter_t *iter, const char *key, uint32_t v_timestamp, uint32_t v_increment, void *data);
bool visit_int64(const bson_iter_t *iter, const char *key, int64_t v_int64, void *data);
bool visit_maxkey(const bson_iter_t *iter, const char *key, void *data);
bool visit_minkey(const bson_iter_t *iter, const char *key, void *data);
void visit_unsupported_type(const bson_iter_t *iter, const char *key, uint32_t type_code, void *data);
bool visit_decimal128(const bson_iter_t *iter, const char *key, const bson_decimal128_t *v_decimal128, void *data);

static const bson_visitor_t visitor = {
	visit_before,
	visit_after,
	visit_corrupt,
	visit_double,
	visit_utf8,
	visit_document,
	visit_array,
	visit_binary,
	visit_undefined,
	visit_oid,
	visit_bool,
	visit_date_time,
	visit_null,
	visit_regex,
	visit_dbpointer,
	visit_code,
	visit_symbol,
	visit_codewscope,
	visit_int32,
	visit_timestamp,
	visit_int64,
	visit_maxkey,
	visit_minkey,
	visit_unsupported_type,
	visit_decimal128
};

/* run before / after descending into a document */
bool visit_before(const bson_iter_t *iter, const char *key, void *data)
{
	return false;
}

bool visit_after(const bson_iter_t *iter, const char *key, void *data)
{
	return false;
}

/* corrupt BSON, or unsupported type and visit_unsupported_type not set */
void visit_corrupt(const bson_iter_t *iter, void *data)
{
	// error
}

/* normal bson field callbacks */
bool visit_double(const bson_iter_t *iter, const char *key, double v_double, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(v_double));
	}
	else
	{
		b->add(VpackValue(v_double));
	}

	return false;
}

bool visit_utf8(const bson_iter_t *iter, const char *key, size_t v_utf8_len, const char *v_utf8, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValuePair(v_utf8, v_utf8_len, VpackValueType::String));
	}
	else
	{
		b->add(VpackValuePair(v_utf8, v_utf8_len, VpackValueType::String));
	}

	return false;
}

bool visit_document(const bson_iter_t *iter, const char *key, const bson_t *v_document, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	bson_iter_t child;

	if(!bson_iter_init(&child, v_document))
	{
		throw std::runtime_error("Unable to initialize BSON iterator (#4)");
	}

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(VpackValueType::Object));
	}
	else
	{
		b->add(VpackValue(VpackValueType::Object));
	}

    bson_iter_visit_all(&child, &visitor, b);

    b->close();

	return false;
}

bool visit_array(const bson_iter_t *iter, const char *key, const bson_t *v_array, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	bson_iter_t child;

	if(!bson_iter_init(&child, v_array))
	{
		throw std::runtime_error("Unable to initialize BSON iterator (#3)");
	}

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(VpackValueType::Array));
	}
	else
	{
		b->add(VpackValue(VpackValueType::Array));
	}

    bson_iter_visit_all(&child, &visitor, b);

    b->close();

	return false;
}

bool visit_binary(const bson_iter_t *iter, const char *key, bson_subtype_t v_subtype, size_t v_binary_len, const uint8_t *v_binary, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	// TODO

	return false;
}

/* DEPRECATED */
/* normal field with deprecated "Undefined" BSON type */
bool visit_undefined(const bson_iter_t *iter, const char *key, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackSlice::noneSlice());
	}
	else
	{
		b->add(VpackSlice::noneSlice());
	}

	return false;
}

bool visit_oid(const bson_iter_t *iter, const char *key, const bson_oid_t *v_oid, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_OBJECTID, VpackValuePair(v_oid->bytes, 12, VpackValueType::Binary));
	}
	else
	{
		b->addTagged(TAG_OBJECTID, VpackValuePair(v_oid->bytes, 12, VpackValueType::Binary));
	}

	return false;
}

bool visit_bool(const bson_iter_t *iter, const char *key, bool v_bool, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(v_bool));
	}
	else
	{
		b->add(VpackValue(v_bool));
	}

	return false;
}

bool visit_date_time(const bson_iter_t *iter, const char *key, int64_t msec_since_epoch, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(msec_since_epoch, VpackValueType::UTCDate));
	}
	else
	{
		b->add(VpackValue(msec_since_epoch, VpackValueType::UTCDate));
	}

	return false;
}

bool visit_null(const bson_iter_t *iter, const char *key, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackSlice::nullSlice());
	}
	else
	{
		b->add(VpackSlice::nullSlice());
	}

	return false;
}

bool visit_regex(const bson_iter_t *iter, const char *key, const char *v_regex, const char *v_options, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_REGEX, VpackValue(VpackValueType::Object));
	}
	else
	{
		b->addTagged(TAG_REGEX, VpackValue(VpackValueType::Object));
	}

	b->add("regex", VpackValuePair(v_regex, strlen(v_regex), VpackValueType::String));
	b->add("modifiers", VpackValuePair(v_regex, strlen(v_options), VpackValueType::String));

	b->close();

	return false;
}

/* DEPRECATED */
bool visit_dbpointer(const bson_iter_t *iter, const char *key, size_t v_collection_len, const char *v_collection, const bson_oid_t *v_oid, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_COLLECTION, VpackValue(VpackValueType::Object));
	}
	else
	{
		b->addTagged(TAG_COLLECTION, VpackValue(VpackValueType::Object));
	}

	b->add("collection", VpackValuePair(v_collection, v_collection_len, VpackValueType::String));
	b->addTagged("id", TAG_OBJECTID, VpackValuePair(v_oid->bytes, 12, VpackValueType::Binary));

	b->close();

	return false;
}

bool visit_code(const bson_iter_t *iter, const char *key, size_t v_code_len, const char *v_code, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_CODE, VpackValuePair(v_code, v_code_len, VpackValueType::String));
	}
	else
	{
		b->addTagged(TAG_CODE, VpackValuePair(v_code, v_code_len, VpackValueType::String));
	}

	return false;
}

/* DEPRECATED */
bool visit_symbol(const bson_iter_t *iter, const char *key, size_t v_symbol_len, const char *v_symbol, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_SYMBOL, VpackValuePair(v_symbol, v_symbol_len, VpackValueType::String));
	}
	else
	{
		b->addTagged(TAG_SYMBOL, VpackValuePair(v_symbol, v_symbol_len, VpackValueType::String));
	}

	return false;
}

bool visit_codewscope(const bson_iter_t *iter, const char *key, size_t v_code_len, const char *v_code, const bson_t *v_scope, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	bson_iter_t child;

	if(!bson_iter_init(&child, v_scope))
	{
		throw std::runtime_error("Unable to initialize BSON iterator (#2)");
	}

	if(b->isOpenObject())
	{
		b->addTagged(key, strlen(key), TAG_CODESCOPE , VpackValue(VpackValueType::Object));
	}
	else
	{
		b->addTagged(TAG_CODESCOPE, VpackValue(VpackValueType::Object));
	}

	b->add("code", VpackValuePair(v_code, v_code_len, VpackValueType::String));

	b->add("scope", VpackValue(VpackValueType::Object));

    bson_iter_visit_all(&child, &visitor, b);

    b->close();

	b->close();

	return false;
}

bool visit_int32(const bson_iter_t *iter, const char *key, int32_t v_int32, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(v_int32));
	}
	else
	{
		b->add(VpackValue(v_int32));
	}

	return false;
}

// internal use only
bool visit_timestamp(const bson_iter_t *iter, const char *key, uint32_t v_timestamp, uint32_t v_increment, void *data)
{
	return false;
}

bool visit_int64(const bson_iter_t *iter, const char *key, int64_t v_int64, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	if(b->isOpenObject())
	{
		b->add(key, strlen(key), VpackValue(v_int64));
	}
	else
	{
		b->add(VpackValue(v_int64));
	}

	return false;
}

// ignore for now
bool visit_maxkey(const bson_iter_t *iter, const char *key, void *data)
{
	return false;
}

// ignore for now
bool visit_minkey(const bson_iter_t *iter, const char *key, void *data)
{
	return false;
}

/* if set, called instead of visit_corrupt when an apparently valid BSON
 * includes an unrecognized field type (reading future version of BSON) */
void visit_unsupported_type(const bson_iter_t *iter, const char *key, uint32_t type_code, void *data)
{
	// error
}

bool visit_decimal128(const bson_iter_t *iter, const char *key, const bson_decimal128_t *v_decimal128, void *data)
{
	VpackBuilder *b = (VpackBuilder*) data;

	// TODO

	char string[BSON_DECIMAL128_STRING];

	bson_decimal128_to_string(v_decimal128, string);

	b->addTagged(key, strlen(key), TAG_DECIMAL, VpackValue(string));

	return false;
}

VpackBuilder* bson_to_builder(char* ptr, size_t len)
{
	VpackBuilder *b = new VpackBuilder();
	bson_iter_t iter;
	bson_t 	    root;

	uint32_t len_le;
	memcpy(&len_le, ptr, sizeof(len_le));

	if(len_le != len)
	{
		throw std::runtime_error("Invalid input length (bson " + std::to_string(len_le) + ", actual " + std::to_string(len) + ")");
	}

	if(!bson_init_static(&root, (uint8_t*) ptr, len))
	{
		throw std::runtime_error("Unable to initialize bson_t from input");
	}

	if(!bson_iter_init(&iter, &root))
	{
		throw std::runtime_error("Unable to initialize BSON iterator (#1)");
	}

	b->openObject();

    bson_iter_visit_all(&iter, &visitor, b);

    b->close();

	bson_destroy(&root);

	return b;
}

void bson_to_builder_append(VpackBuilder &b, char* ptr, size_t len)
{
	bson_iter_t iter;
	bson_t 	    root;

	uint32_t len_le;
	memcpy(&len_le, ptr, sizeof(len_le));

	if(len_le != len)
	{
		throw std::runtime_error("Invalid input length (bson " + std::to_string(len_le) + ", actual " + std::to_string(len) + ")");
	}

	if(!bson_init_static(&root, (uint8_t*) ptr, len))
	{
		throw std::runtime_error("Unable to initialize bson_t from input");
	}

	if(!bson_iter_init(&iter, &root))
	{
		throw std::runtime_error("Unable to initialize BSON iterator (#1)");
	}

	b.openObject();

    bson_iter_visit_all(&iter, &visitor, &b);

    b.close();

	bson_destroy(&root);
}


PG_FUNCTION_INFO_V1(bson_to_vpack);

Datum
bson_to_vpack(PG_FUNCTION_ARGS)
{
	VPACK_TRY

	bytea	   *bytes = PG_GETARG_BYTEA_P(0);
	VpackBuilder *b = bson_to_builder(VARDATA(bytes), VARSIZE(bytes) - VARHDRSZ);

	PG_RETURN_VPACK(builder_to_vpack(b));

	VPACK_PG_CATCH
}


} // extern "C"
