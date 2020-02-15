#ifndef MEMORY_H_
#define MEMORY_H_

extern "C" {
#include "postgres.h"
}

#include "memory.h"
#include <new>
#include <iostream>
#include <inttypes.h>
#include <stdint.h>
#include <unistd.h>

extern "C" {
void* _palloc0(std::size_t size);
void* _palloc(std::size_t size);
void* _repalloc(void* ptr, std::size_t size);
void _pfree(void* ptr);
}

/*
 * Make "new" and "delete" use palloc0 and pfree
 *
 * Causes issues when passing by value
 */

void*
operator new(std::size_t size);

void*
operator new[](std::size_t size);

void
operator delete(void *ptr) throw();

void
operator delete[](void *ptr) throw();

void*
operator new(std::size_t size, const std::nothrow_t&) throw();

void*
operator new[](std::size_t size, const std::nothrow_t&) throw();

void
operator delete(void *ptr, const std::nothrow_t&) throw();

void
operator delete[](void *ptr, const std::nothrow_t&) throw();

#endif
