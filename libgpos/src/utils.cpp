//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		utils.cpp
//
//	@doc:
//		Various utilities which are not necessarily gpos specific
//---------------------------------------------------------------------------
#include "gpos/utils.h"

#include <cstring>

#include "gpos/types.h"

// using 16 addresses a line fits exactly into 80 characters
#define GPOS_MEM_BPL 16


// number of stack frames to search for addresses
#define GPOS_SEARCH_STACK_FRAMES 16


using namespace gpos;

//---------------------------------------------------------------------------
//	GPOS versions of standard streams
//	Do not reference std::(w)cerr/(w)cout directly;
//---------------------------------------------------------------------------
COstreamBasic gpos::oswcerr(&std::wcerr);
COstreamBasic gpos::oswcout(&std::wcout);


//---------------------------------------------------------------------------
//	@function:
//		gpos::Print
//
//	@doc:
//		Print wide-character string
//
//---------------------------------------------------------------------------
void
gpos::Print(WCHAR *wsz)
{
	std::wcout << wsz;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::HexDump
//
//	@doc:
//		Generic memory dumper; produces regular hex dump
//
//---------------------------------------------------------------------------
IOstream &
gpos::HexDump(IOstream &os, const void *pv, ULLONG size)
{
	for (ULONG i = 0; i < 1 + (size / GPOS_MEM_BPL); i++)
	{
		// starting address of line
		BYTE *buf = ((BYTE *) pv) + (GPOS_MEM_BPL * i);
		os << (void *) buf << "  ";
		os << COstream::EsmHex;

		// individual bytes
		for (ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			if (buf[j] < 16)
			{
				os << "0";
			}

			os << (ULONG) buf[j] << " ";

			// separator in middle of line
			if (j + 1 == GPOS_MEM_BPL / 2)
			{
				os << "- ";
			}
		}

		// blank between hex and text dump
		os << " ";

		// text representation
		for (ULONG j = 0; j < GPOS_MEM_BPL; j++)
		{
			// print only 'visible' characters
			if (buf[j] >= 0x20 && buf[j] <= 0x7f)
			{
				// cast to CHAR to avoid stream from (mis-)interpreting BYTE
				os << (CHAR) buf[j];
			}
			else
			{
				os << ".";
			}
		}
		os << COstream::EsmDec << std::endl;
	}
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::HashByteArray
//
//	@doc:
//		Generic hash function for an array of BYTEs.
//
//		The original implementation was a byte-at-a-time Knuth-style mixer
//		(loop body: hash = ((hash << 5) ^ (hash >> 27)) ^ b).  At TPC-H sf=5
//		Q21 planning, this function alone was 4.85% of CPU samples (with
//		many more inside CRefCount/memo dedup paths that call into it via
//		HashValue() on CMDId / CColRef / CBitVector).  The byte-at-a-time
//		loop also has poor avalanche, causing extra memo-bucket collisions.
//
//		Replaced with a 64-bit FNV-1a variant: process 8 bytes per
//		iteration via unaligned loads, fold tail bytes one at a time.
//		Output is truncated to ULONG after a final xor-fold; this keeps
//		callers (which key memo / hashmap buckets by ULONG) unchanged.
//---------------------------------------------------------------------------
ULONG
gpos::HashByteArray(const BYTE *byte_array, ULONG size)
{
	const ULLONG FNV_OFFSET = 0xcbf29ce484222325ULL;
	const ULLONG FNV_PRIME = 0x100000001b3ULL;

	ULLONG h = FNV_OFFSET ^ static_cast<ULLONG>(size);

	ULONG i = 0;
	while (i + 8 <= size)
	{
		ULLONG word;
		// unaligned load is safe on x86_64 and ARMv8
		std::memcpy(&word, byte_array + i, sizeof(word));
		h ^= word;
		h *= FNV_PRIME;
		i += 8;
	}
	for (; i < size; ++i)
	{
		h ^= static_cast<ULLONG>(byte_array[i]);
		h *= FNV_PRIME;
	}

	// fold 64 -> 32 so the output domain matches ULONG callers
	return static_cast<ULONG>(h ^ (h >> 32));
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::CombineHashes
//
//	@doc:
//		Combine ULONG-based hash values.  Specialized to avoid going through
//		the byte-array path with sizeof(ULONG)*2 = 8 bytes -- direct integer
//		mixing is several times faster and is on the hot path for memo and
//		expression hashing.
//
//---------------------------------------------------------------------------
ULONG
gpos::CombineHashes(ULONG hash1, ULONG hash2)
{
	const ULLONG FNV_PRIME = 0x100000001b3ULL;
	ULLONG h = (static_cast<ULLONG>(hash1) << 32) | static_cast<ULLONG>(hash2);
	h ^= 0xcbf29ce484222325ULL;
	h *= FNV_PRIME;
	return static_cast<ULONG>(h ^ (h >> 32));
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::Add
//
//	@doc:
//		Add two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG
gpos::Add(ULLONG first, ULLONG second)
{
	if (first > gpos::ullong_max - second)
	{
		// if addition result overflows, we have (a + b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max - b)
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOverflow);
	}

	ULLONG res = first + second;

	return res;
}


//---------------------------------------------------------------------------
//	@function:
//		gpos::Multiply
//
//	@doc:
//		Multiply two unsigned long long values, throw an exception if overflow occurs,
//
//---------------------------------------------------------------------------
ULLONG
gpos::Multiply(ULLONG first, ULLONG second)
{
	if (0 < second && first > gpos::ullong_max / second)
	{
		// if multiplication result overflows, we have (a * b > gpos::ullong_max),
		// then we need to check for  (a > gpos::ullong_max / b)
		GPOS_RAISE(CException::ExmaSystem, CException::ExmiOverflow);
	}
	ULLONG res = first * second;

	return res;
}

// EOF
