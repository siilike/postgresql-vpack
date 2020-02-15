#
# https://github.com/maciekgajewski/postgresbson
#
# Copyright (c) 2012 Maciej Gajewski <maciej.gajewski0@gmail.com>
#
# Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted,
# provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.
#
# IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
# ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
# THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#

if(NOT Postgres_INCLUDEDIR OR NOT Postgres_LIBDIR OR NOT Postgres_EXTENSIONDIR)
	message(STATUS "Looking for PostgreSQL...")

	execute_process(
		COMMAND pg_config --includedir-server
		OUTPUT_VARIABLE OUT_Postgres_INCLUDEDIR OUTPUT_STRIP_TRAILING_WHITESPACE
	)

	if(NOT OUT_Postgres_INCLUDEDIR)
		set(Postgres_FOUND FALSE)
		message(FATAL_ERROR "pg_config unavailable. Make sure it is in your path")
	else()
		set(Postgres_FOUND TRUE)
	endif()

	execute_process(
		COMMAND pg_config --pkglibdir
		OUTPUT_VARIABLE OUT_Postgres_LIBDIR OUTPUT_STRIP_TRAILING_WHITESPACE
	)

	execute_process(
		COMMAND pg_config --sharedir
		OUTPUT_VARIABLE OUT_Postgres_SHAREDIR OUTPUT_STRIP_TRAILING_WHITESPACE
	)

	set(Postgres_INCLUDEDIR ${OUT_Postgres_INCLUDEDIR} CACHE PATH "Path to PostgreSQL server includes")
	set(Postgres_LIBDIR ${OUT_Postgres_LIBDIR} CACHE PATH "Path to PostgreSQL library")
	set(Postgres_EXTENSIONDIR "${OUT_Postgres_SHAREDIR}/extension" CACHE PATH "Path to extension dir")
else()
	message(STATUS "PostgreSQL location loaded from cache")
	set(Postgres_FOUND TRUE)
endif()

if(Postgres_FOUND)
	message(STATUS "PostgreSQL found: includedir=${Postgres_INCLUDEDIR}, pkglibdir=${Postgres_LIBDIR}, extensiondir=${Postgres_EXTENSIONDIR}")
endif()
