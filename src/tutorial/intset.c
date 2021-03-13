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
