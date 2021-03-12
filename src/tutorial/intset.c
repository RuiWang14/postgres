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

PG_MODULE_MAGIC;

typedef struct
{
	int32 vl_len_; /* varlena header (do not touch directly!) */
	int32 size;
	int32 data[FLEXIBLE_ARRAY_MEMBER];
} IntSet;

typedef struct {
    int size;
    int len;
    int *data;
} IntSetInternal;

IntSet *newIntSet(int size)
{
	IntSet *new = (IntSet *)palloc(VARHDRSZ + sizeof(int32) * (size + 1));
	SET_VARSIZE(new, VARHDRSZ + sizeof(int32) * (size + 1));
	new->size = size;
	return new;
}

IntSetInternal *newIntSetInternal(int size) {

    IntSetInternal *new = (IntSetInternal *)palloc(sizeof(IntSetInternal));
    if (new == NULL) {
        perror("new IntSetInternal");
        return NULL;
    }

    new->data = (int32 *) palloc(size * sizeof(int32));
    if (new->data == NULL) {
        perror("new IntSetInternal data");
        free(new);
        return NULL;
    }

    new->size = size;
    new->len = 0;
    return new;
}

int add(IntSetInternal *list, int val)
{
	// double array size
    if (list->len >= list->size) {
        int *oldArray = list->data;

        int *newArray = (int *) palloc(list->size * 2 * sizeof(int32));
        if (newArray == NULL) {
            perror("double number list size");
            return -1;
        }

        // copy data
        for (int i = 0; i < list->size; i++) {
            newArray[i] = oldArray[i];
        }

        list->data = newArray;
        list->size *= 2;
    }
	list->data[list->len] = val;
	list->len++;
	return 0;
}

int convert2Number(char input) {
    int num = input - '0';
    if (num < 0 || num > 9) {
        return -1;
    } else {
        return num;
    }
}

IntSet *newIntSetFromString(char *input) {

    int leftBrace = 0;
    int commaFlag = 0;
    int errorInput = 0;
    int hasNumber = 0;

    IntSetInternal *list = newIntSetInternal(4);

    int number = 0;
    for (char *ch = input; *ch != '\0'; ch++) {
        switch (*ch) {
            case '{': {
                leftBrace++;
                break;
            }
            case '}': {
                if (leftBrace != 1) {
                    errorInput = 1;
                    break;
                }
                if(hasNumber > 0) {
                    add(list, number);
                    number = 0;
                }
                break;
            }
            case ' ':
                break;
            case ',': {
                if (commaFlag > 0) {
                    errorInput = 1;
                    break;
                }
                add(list, number);
                commaFlag = 1;
                number = 0;
                break;
            }
            default: {
                int num = convert2Number(*ch);
                if (num < 0) {
                    errorInput = 1;
                    break;
                } else {
                    hasNumber = 1;
                    number = number * 10 + num;
                    commaFlag = 0;
                }
                break;
            }
        }
    }

    if (errorInput > 0 || commaFlag > 0) {
        printf("error input\n");
        return NULL;
    }

    IntSet *set = newIntSet(list->len);
    for (int i = 0; i < list->len; i++) {
        set->data[i] = list->data[i];
    }

    return set;
}

char *toString(IntSet *intSet) {
    if(intSet == NULL || intSet -> size <= 0){
        return "{}";
    }

    char *str = malloc(sizeof(char) * ((intSet->size) * 2 + 2));
    char *p = str;
    *p++ = '{';
    for (int i = 0; ;) {
        *p++ = intSet->data[i] + '0';
        i++;
        if (i < intSet->size){
            *p++ = ',';
        }else{
            break;
        }
    }
    *p++ = '}';
    *p = '\0';
    return str;
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
