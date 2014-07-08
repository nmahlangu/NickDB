// a struct for storing intermediate results
typedef struct intermediateResult
{
	char* variableName;
	int* validPositions;
	int numberOfValidPositions;
	struct intermediateResult* next;
}intermediateResult;
intermediateResult* intermediateResultRoot = NULL;

// inserts an intermediate result into a linked list
void insertIntermediateResultIntoLinkedList(intermediateResult* variable)
{
	// if list is empty
	if (intermediateResultRoot == NULL)
	{
		intermediateResultRoot = variable;
	}

	// if list is not empty, insert the new element at the head of it
	else
	{
		variable->next = intermediateResultRoot;
		intermediateResultRoot = variable;
	}
}

// checks if an intermediate result is in a linked list
intermediateResult* checkForIntermediateResultInLinkedList(char* variableName)
{
	intermediateResult* trav = intermediateResultRoot;
	while (trav != NULL)
	{
		// see if names match
		if (strcmp(variableName, trav->variableName) == 0)
		{
			return trav;
		}

		// update pointer
		trav = trav->next;
	}
	return NULL;
}

// simply prints a linked list (for debugging purposes)
void printLinkedListOfIntermediateResults(void)
{
	intermediateResult* trav = intermediateResultRoot;
	while (trav != NULL)
	{
		printf("variableName: [%s]\n", trav->variableName);
		printf("numberOfValidPositions: [%d]\n", trav->numberOfValidPositions);
		printf("[");
		for (int i = 0; i < trav->numberOfValidPositions; i++)
		{
			if (i + 1 != trav->numberOfValidPositions)
				printf("%d,", trav->validPositions[i]);
			else
				printf("%d]\n", trav->validPositions[i]);
		}
		trav = trav->next;
	}
	printf("End of list\n");
}