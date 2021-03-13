/*
 * src/tutorial/complex.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h" /* needed for send/recv functions */
#include <string.h>

PG_MODULE_MAGIC;

typedef struct
{
	int32 vl_len_; /* varlena header (do not touch directly!) */
	int32 size;
	int32 data[FLEXIBLE_ARRAY_MEMBER];
} IntSet;

typedef struct
{
	int size;
	int len;
	int *data;
} IntSetInternal;

IntSet *newIntSet(int size)
{
	IntSet *new = (IntSet *)palloc(VARHDRSZ + sizeof(int32) * (size + 1));
	if (new == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc newIntSet new")));
	}
	SET_VARSIZE(new, VARHDRSZ + sizeof(int32) * (size + 1));
	new->size = size;
	return new;
}

IntSetInternal *newIntSetInternal(int size)
{

	IntSetInternal *new = (IntSetInternal *)palloc(sizeof(IntSetInternal));
	if (new == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc newIntSetInternal")));
		return NULL;
	}

	new->data = (int32 *)palloc(size * sizeof(int32));
	if (new->data == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc newIntSetInternal->data")));
		return NULL;
	}

	new->size = size;
	new->len = 0;
	return new;
}

int add(IntSetInternal *list, int val)
{
	// double array size
	if (list->len >= list->size)
	{
		int *oldArray = list->data;

		int *newArray = (int *)palloc(list->size * 2 * sizeof(int32));
		if (newArray == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
					 errmsg("error palloc double size array")));
			return -1;
		}

		// copy data
		memcpy(newArray, oldArray, sizeof(int) * list->len);
		list->data = newArray;
		list->size *= 2;

		pfree(oldArray);
	}
	for (int i = 0; i < list->len; i++)
	{
		if (list->data[i] == val)
		{
			return 0;
		}
	}
	list->data[list->len] = val;
	list->len++;
	return 0;
}

int convert2Number(char input)
{
	int num = input - '0';
	if (num < 0 || num > 9)
	{
		return -1;
	}
	else
	{
		return num;
	}
}

IntSet *newIntSetFromString(char *input)
{

	int leftBrace = 0;
	int rightBrace = 0;
	bool hasComma = false;
	bool hasNumber = false;
	enum
	{
		NoCommit,
		Blank,
		Comma,
		RightBrace
	} committer = NoCommit;

	IntSetInternal *list = newIntSetInternal(32);

	int number = 0;
	for (char *ch = input; *ch != '\0'; ch++)
	{
		switch (*ch)
		{
		case '{':
		{
			leftBrace++;
			break;
		}
		case '}':
		{
			rightBrace++;
			if (rightBrace > 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("error input: too many rightBrace")));
			}
			if (leftBrace != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("error input: wrong leftBrace")));
			}
			if (hasNumber == true)
			{
				add(list, number);
				hasNumber = false;
				number = 0;
				committer = RightBrace;
			}
			break;
		}
		case ' ':
		{
			if (hasNumber == true)
			{
				add(list, number);
				hasNumber = false;
				number = 0;
				committer = Blank;
			}
			break;
		}

		case ',':
		{
			if (hasComma == true)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("error input: have ,,")));
			}
			if (hasNumber == true)
			{
				add(list, number);
				hasNumber = false;
				number = 0;
				committer = Comma;
			}
			hasComma = true;
			break;
		}
		default:
		{
			int num = convert2Number(*ch);
			if (num < 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("error input: wrong letter")));
			}
			else
			{
				hasNumber = true;
				number = number * 10 + num;
				if (number < 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("error input: number too large, overflow")));
				}
				if (committer != NoCommit)
				{
					if (committer != Comma && hasComma == false)
					{
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg(psprintf("error input: %s\n", input))));
					}
				}
				hasComma = false;
			}
			break;
		}
		}
	}

	if (hasComma == true || rightBrace != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("error input: end with comma or wrong rightBrace")));
	}

	IntSet *set = newIntSet(list->len);
	memcpy(set->data, list->data, sizeof(int) * list->len);

	pfree(list->data);
	pfree(list);

	return set;
}

char *toString(IntSet *intSet)
{
	if (intSet == NULL || intSet->size <= 0)
	{
		return "{}";
	}

	char *str = palloc(sizeof(char) * (strlen("\0")));
	if (str == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc str")));
	}
	str = psprintf("\0");
	char *number = palloc(sizeof(char) * (strlen("2147483647") + strlen("\0")));
	;
	if (number == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc string number")));
	}
	for (int i = 0; i < intSet->size; i++)
	{
		number = psprintf("%d", intSet->data[i]);
		char *temp;
		if (strlen(str) == 0)
		{
			temp = palloc(sizeof(char) * (strlen(number) + strlen("\0")));
			if (temp == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						 errmsg("error palloc string temp")));
			}
			temp = psprintf("%s", number);
		}
		else
		{
			temp = palloc(sizeof(char) * (strlen(str) + strlen(",") + strlen(number) + strlen("\0")));
			if (temp == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
						 errmsg("error palloc string temp")));
			}
			temp = psprintf("%s,%s", str, number);
		}
		pfree(str);
		str = temp;
	}
	char *result = palloc(sizeof(char) * (strlen("{}") + strlen(str) + strlen("\0")));
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("error palloc string result")));
	}
	result = psprintf("{%s}", str);
	pfree(str);
	pfree(number);

	return result;
}

bool contain(IntSet *setA, IntSet *setB)
{
	for (int i = 0; i < setB->size; i++)
	{
		bool contain = false;
		for (int j = 0; j < setA->size; j++)
		{
			if (setB->data[i] == setA->data[j])
			{
				contain = true;
				break;
			}
		}
		if (contain == false)
		{
			return false;
		}
	}

	return true;
}

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(intset_in);

Datum
	intset_in(PG_FUNCTION_ARGS)
{
	char *str = PG_GETARG_CSTRING(0);
	IntSet *result = newIntSetFromString(str);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(intset_out);

Datum
	intset_out(PG_FUNCTION_ARGS)
{
	IntSet *set = (IntSet *)PG_GETARG_POINTER(0);
	char *result = toString(set);
	PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * New Operators
 * 
 *****************************************************************************/

/*
 * in
 * 1 ? {1,2} -> true
 */

PG_FUNCTION_INFO_V1(intset_ein);

Datum
	intset_ein(PG_FUNCTION_ARGS)
{
	int32 num = PG_GETARG_INT32(0);
	IntSet *set = (IntSet *)PG_GETARG_POINTER(1);

	for (int i = 0; i < set->size; i++)
	{
		if (num == set->data[i])
		{
			PG_RETURN_BOOL(true);
		}
	}
	PG_RETURN_BOOL(false);
}

/*
 * cardinality
 * |{1,2}| -> 2
 */

PG_FUNCTION_INFO_V1(intset_card);

Datum
	intset_card(PG_FUNCTION_ARGS)
{
	IntSet *set = (IntSet *)PG_GETARG_POINTER(0);
	PG_RETURN_INT32(set->size);
}

/*
 * contain
 * A >@ B 
 * A contain B
 */
PG_FUNCTION_INFO_V1(intset_contain);

Datum
	intset_contain(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	bool result = contain(setA, setB);

	PG_RETURN_BOOL(result);
}

/*
 * contain
 * A >@ B 
 * A contain B
 */
PG_FUNCTION_INFO_V1(intset_subset);

Datum
	intset_subset(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	bool result = contain(setB, setA);

	PG_RETURN_BOOL(result);
}

/*
 * equal
 * A = B 
 */
PG_FUNCTION_INFO_V1(intset_equal);

Datum
	intset_equal(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	bool AContainB = contain(setA, setB);
	bool BContainA = contain(setB, setA);

	PG_RETURN_BOOL(AContainB && BContainA);
}

/*
 * not equal
 * A <> B 
 */
PG_FUNCTION_INFO_V1(intset_notequal);

Datum
	intset_notequal(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	bool AContainB = contain(setA, setB);
	bool BContainA = contain(setB, setA);

	PG_RETURN_BOOL(!(AContainB && BContainA));
}

/*
 * intersection
 * A AND B 
 */
PG_FUNCTION_INFO_V1(intset_intersection);

Datum
	intset_intersection(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	int minSize = setA->size > setB->size ? setB->size : setA->size;

	int *list = palloc(sizeof(int32) * minSize);
	int p = 0;

	for (int i = 0; i < setA->size; i++)
	{
		for (int j = 0; j < setB->size; j++)
		{
			if (setA->data[i] == setB->data[j])
			{
				list[p++] = setA->data[i];
			}
		}
	}

	IntSet *set = newIntSet(minSize);
	memcpy(set->data, list, sizeof(int) * minSize);
	pfree(list);

	PG_RETURN_POINTER(set);
}

/*
 * union
 * A OR B 
 */
PG_FUNCTION_INFO_V1(intset_union);

Datum
	intset_union(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	int maxSize = setA->size + setB->size;

	int *list = palloc(sizeof(int32) * maxSize);
	memcpy(list, setB->data, sizeof(int32) * setB->size);
	int len = setB->size;

	for (int i = 0; i < setA->size; i++)
	{
		bool find = false;
		for (int j = 0; j < setB->size; j++)
		{
			if (setA->data[i] == setB->data[j])
			{
				find = true;
				break;
			}
		}
		if (find == false)
		{
			list[len++] = setA->data[i];
		}
	}

	IntSet *set = newIntSet(len);
	memcpy(set->data, list, sizeof(int32) * len);
	pfree(list);

	PG_RETURN_POINTER(set);
}

/*
 * xor
 * A XOR B 
 */
PG_FUNCTION_INFO_V1(intset_xor);

Datum
	intset_xor(PG_FUNCTION_ARGS)
{
	IntSet *setA = (IntSet *)PG_GETARG_POINTER(0);
	IntSet *setB = (IntSet *)PG_GETARG_POINTER(1);

	int maxSize = setA->size + setB->size;

	int *list = palloc(sizeof(int32) * maxSize);
	int len = 0;

	for (int i = 0; i < setA->size; i++)
	{
		bool find = false;
		for (int j = 0; j < setB->size; j++)
		{
			if (setA->data[i] == setB->data[j])
			{
				find = true;
				break;
			}
		}
		if (find == false)
		{
			list[len++] = setA->data[i];
		}
	}

	for (int i = 0; i < setB->size; i++)
	{
		bool find = false;
		for (int j = 0; j < setA->size; j++)
		{
			if (setB->data[i] == setA->data[j])
			{
				find = true;
				break;
			}
		}
		if (find == false)
		{
			list[len++] = setB->data[i];
		}
	}

	IntSet *set = newIntSet(len);
	memcpy(set->data, list, sizeof(int32) * len);
	pfree(list);

	PG_RETURN_POINTER(set);
}