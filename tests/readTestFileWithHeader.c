/*
 *  Just a script that reads the header of a database file.
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define UNSORTED 1
#define SORTED 2
#define BTREE 3

int main(int argc, char** argv)
{
	// make sure a file was passed in
	if (argc != 2)
	{
		printf("Usage: ./readTestFileWithHeader testFileName\n");
		return 1;
	}

	// read its storage type and make sure it's valid	
	FILE* fp = fopen(argv[1], "rb");
	if (fp == NULL)
	{
		printf("File was not valid.\n");
		return 2;
	}
	int storageType;
	fread(&storageType, sizeof(int), 1, fp);
	assert((storageType == UNSORTED) || (storageType == SORTED) || (storageType == BTREE));
	if (storageType == UNSORTED)
		printf("Storage type is UNSORTED (1)\n");
	else if (storageType == SORTED)
		printf("Storage type is SORTED (2)\n");
	else if (storageType == BTREE)
		printf("Storage type is BTREE (3)\n");
	else
		abort();

	// read the file size
	int fileSize;
	fread(&fileSize, sizeof(int), 1, fp);	
	printf("File size is: %d bytes\n", fileSize);

	// read the data
	int* fileData = malloc(fileSize);
	fread(fileData, fileSize, 1, fp);

	// print the data
	for (int i = 0; i < (fileSize / 4); i++)
	{
		printf("%d-", fileData[i]);
		fflush(stdout);
	}
	printf("EOF\n");
}
