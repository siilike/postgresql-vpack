
#include "memory.h"
#include "postgres.h"
#include "utils/palloc.h"

#include <new>
#include <iostream>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

/*
 * Make "new" and "delete" use palloc0 and pfree
 *
 * Causes issues when passing by value
 */

extern "C" {

void* _palloc0(std::size_t size)
{
	return palloc0(size);
}

void* _palloc(std::size_t size)
{
	return palloc(size);
}

void* _repalloc(void* ptr, std::size_t size)
{
	return repalloc(ptr, size);
}

void _pfree(void* ptr)
{
	pfree(ptr);
}

}

void*
operator new(std::size_t size)
{
	void* ret = _palloc0(size);

#ifdef DEBUG_MEMORY
	printf("new %d 0x%" PRIXPTR "\n", size, (uintptr_t)ret);
#endif

	return ret;
}

void*
operator new[](std::size_t size)
{
	void* ret = _palloc0(size);

#ifdef DEBUG_MEMORY
	printf("new[] %d 0x%" PRIXPTR "\n", size, (uintptr_t)ret);
#endif

	return ret;
}

void
operator delete(void *ptr) throw()
{
#ifdef DEBUG_MEMORY
	printf("delete 0x%" PRIXPTR "\n", (uintptr_t)ptr);
#endif

#ifdef ENABLE_FREE
	_pfree(ptr);
#endif
}

void
operator delete[](void *ptr) throw()
{
#ifdef DEBUG_MEMORY
	printf("delete[] 0x%" PRIXPTR "\n", (uintptr_t)ptr);
#endif

#ifdef ENABLE_FREE
	_pfree(ptr);
#endif
}

void*
operator new(std::size_t size, const std::nothrow_t&) throw()
{
	void* ret = _palloc0(size);

#ifdef DEBUG_MEMORY
	printf("new %d 0x%" PRIXPTR "\n", size, (uintptr_t)ret);
#endif

	return ret;
}

void*
operator new[](std::size_t size, const std::nothrow_t&) throw()
{
	void* ret = _palloc0(size);

#ifdef DEBUG_MEMORY
	printf("new[] %d 0x%" PRIXPTR "\n", size, (uintptr_t)ret);
#endif

	return ret;
}

void
operator delete(void *ptr, const std::nothrow_t&) throw()
{
#ifdef DEBUG_MEMORY
	printf("delete[] 0x%" PRIXPTR "\n", (uintptr_t)ptr);
#endif

#ifdef ENABLE_FREE
	_pfree(ptr);
#endif
}

void
operator delete[](void *ptr, const std::nothrow_t&) throw()
{
#ifdef DEBUG_MEMORY
	printf("delete[] 0x%" PRIXPTR "\n", (uintptr_t)ptr);
#endif

#ifdef ENABLE_FREE
	_pfree(ptr);
#endif
}
