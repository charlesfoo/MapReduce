#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include "mapreduce.h"



#define Pthread_create(thread,attr,start_routine,arg) assert(pthread_create(thread,attr,start_routine,arg)==0);
#define Pthread_join(thread,value_ptr) assert(pthread_join(thread,value_ptr)==0);
#define Pthread_mutex_lock(m) assert(pthread_mutex_lock(m)==0);
#define Pthread_mutex_unlock(m) assert(pthread_mutex_unlock(m)==0);

#define SIZE_HASHTABLE (67723)
#define SIZE_VALUESARRAY (5000)

#define true 1
#define false 0



struct KeyValuePair
{
	char* key;
	char** values;
	int numOfElementsInValuesArray;
}__attribute__((packed));


struct KeyValueNode
{
	char* key;
	char** values;
	int numOfElementsInValuesArray;
	int sizeOfValuesArray;
	struct KeyValueNode *next;
}__attribute__((packed));


struct HashTableBucket
{
	struct KeyValueNode *head;
	//number of keys in this HashTableBucket (number of nodes in the linkedlist)
	pthread_mutex_t bucketLock;
}__attribute__((packed));



struct Partition
{
	//hashtable stores an array of KeyValueNode
	/**
	* 	Design
	*
	*
	*     KeyValueNode -> KeyValueNode -> KeyValueNode -> ...
	*        ^
	*        |
	*     ___________________
	*     |    |    |    |   |  Hash Table
	*     |____|____|____|___|
	*
	*        ^			 
	*        |
	*   _________________________________________________________________________
	*   |           |           |           |           |           |           |
	*   |           |           |           |           |           |           |
	*   |           |           |           |           |           |           |  Partition
	*   |           |           |           |           |           |           |
	*   |___________|___________|___________|___________|___________|___________|
	*        P1           P2         P3          P4          P5          P6
	*/ 
	struct HashTableBucket **hashTable;
	int numKeys;
	struct KeyValuePair** sortedKeyValuePairArray;
	int currentArrayCounter;
	int valueCounterForCurrentKeyValuePair;
	pthread_mutex_t partitionLock;
}__attribute__((packed));

typedef struct KeyValueNode KeyValueNode;
typedef struct HashTableBucket HashTableBucket;
typedef struct Partition Partition;
typedef struct KeyValuePair KeyValuePair;


//variables for grabbing files
char** files;
pthread_mutex_t filetableLock;
int numFiles;
int numOfFilesLeft;

//Variables for partition
Partition **partitionArray;
int numOfPartition;
//Variables for mappers and reducers
Mapper _Map;
Reducer _Reduce;
Partitioner _Partition;

////////////////////////////////////////////////////////////////////////////////////

/*
 * This hash function is called sdbm. Reference http://www.cse.yorku.ca/~oz/hash.html
 *
**/
unsigned long hash_function(char* key, int sizeOfHashtable)
{
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash%sizeOfHashtable);
}



char* get_next(char* key, int partition_number)
{
	Partition* targetPartition=partitionArray[partition_number];
	KeyValuePair** sortedKeyValuePairArray=targetPartition->sortedKeyValuePairArray;
	KeyValuePair* currentKeyValuePair=sortedKeyValuePairArray[targetPartition->currentArrayCounter];

	assert(strcmp(currentKeyValuePair->key,key)==0);


	char** valuesArray=currentKeyValuePair->values;
	int valuesArrayCounter=targetPartition->valueCounterForCurrentKeyValuePair;
	int sizeOfValuesArray=currentKeyValuePair->numOfElementsInValuesArray;

	if(valuesArrayCounter>=sizeOfValuesArray)
	{
		return NULL;
	}
	
	if(valuesArrayCounter>0)
	{
		free(currentKeyValuePair->values[valuesArrayCounter-1]);
	}

	char* currentValue=valuesArray[valuesArrayCounter];
	targetPartition->valueCounterForCurrentKeyValuePair=targetPartition->valueCounterForCurrentKeyValuePair+1;
	return currentValue;
}



void* GrabFileAndMap()
{
	int i; char* filename;

	grabNewFile:

		filename=NULL;

		//Note: mapper threads should repetitively come here until there's no file left to map.
		//We first grab filetable lock. Check if there's still any file to Map().
		//if there is, grab the file from files[i], set files[i]=NULL, map() the file
		//else, just exit the thread

		Pthread_mutex_lock(&filetableLock);


		//if numOfFiles left is zero, no need to go into loop anymore
		if(numOfFilesLeft==0)
		{

			Pthread_mutex_unlock(&filetableLock);
			pthread_exit(0);
		}

		for(i=0;i<numFiles;i++)
		{
			if(files[i]!=NULL)
			{
				filename=strdup(files[i]);
				free(files[i]);
				files[i]=NULL;
				numOfFilesLeft--;
				break;
			}

		}


		Pthread_mutex_unlock(&filetableLock);


		//if the mapper thread is unable to grab a file, exit it
		assert(filename!=NULL);
		// if(filename==NULL)
		// {
		// 	printf("Something funny happened here.\n");
		// }

		//////////////////////////////////



	_Map(filename);
	
	free(filename);

	if(numOfFilesLeft==0)
		pthread_exit(0);
	else
		goto grabNewFile;


}

/*  Store <key,value> into partition
 *  If key already exists, append the value to the end of existing value list.
 *  Else, find the partition this <key,value> should go to and hash it into the partition's hash table.
 **/
void MR_Emit(char *key, char *value)
{
	unsigned long partitionNumber=_Partition(key, numOfPartition);
	unsigned long hashtablePosition=hash_function(key,SIZE_HASHTABLE);

	Partition *targetPartition=partitionArray[partitionNumber];
	HashTableBucket* targetBucket=targetPartition->hashTable[hashtablePosition];

	//creates a keyValueNode, insert it if the key doesn't exist in the linkedlist in the hashtable bucket yet. free this if it already exist
	KeyValueNode* keyValue=malloc(sizeof(KeyValueNode));
	keyValue->key=strdup(key);																			
	keyValue->values=malloc(SIZE_VALUESARRAY*sizeof(char*));
	keyValue->values[0]=strdup(value);
	keyValue->numOfElementsInValuesArray=1;
	keyValue->sizeOfValuesArray=SIZE_VALUESARRAY;
	keyValue->next=NULL;
	


	pthread_mutex_lock(&(targetBucket->bucketLock));

	if(targetBucket->head==NULL)
	{
		targetBucket->head=keyValue;

		//FATAL PROBLEM: If bucket 1 and bucket 2 of the same partition (different thread) want to set the head to keyValueNode, 
		//they will need to BOTH update to the common shared variable of targetPartition->numKeys. We need a partition lock here.       //VVVVIP

		pthread_mutex_unlock(&(targetBucket->bucketLock));

		pthread_mutex_lock(&(targetPartition->partitionLock));
		targetPartition->numKeys=targetPartition->numKeys+1;  
		pthread_mutex_unlock(&(targetPartition->partitionLock));

		return;
	}
	else
	{
		KeyValueNode* curr=targetBucket->head;
		//check for first node in linked list
		if(strcmp(curr->key,key)==0)
		{
			int numOfElements=curr->numOfElementsInValuesArray;
			int capacity=curr->sizeOfValuesArray;
			//if the valuesArray is full, expand it by twice its size
			if(numOfElements>=capacity)
			{
				curr->values=realloc(curr->values,(2*capacity)*sizeof(char*));
				curr->sizeOfValuesArray=2*capacity;
			}
			curr->values[numOfElements]=strdup(value);
			curr->numOfElementsInValuesArray=curr->numOfElementsInValuesArray+1;

			pthread_mutex_unlock(&(targetBucket->bucketLock));

			free(keyValue->key);
			free(keyValue->values[0]);
			free(keyValue->values);
			free(keyValue);
			return;
		}
		//check for second node onwards in linked list
		while(curr->next!=NULL)
		{
			if(strcmp(curr->next->key,key)==0)
			{
				int numOfElements=curr->next->numOfElementsInValuesArray;
				int capacity=curr->next->sizeOfValuesArray;
				//if the valuesArray is full, expand it by twice its size
				if(numOfElements>=capacity)
				{
					curr->next->values=realloc(curr->next->values,(2*capacity)*sizeof(char*));
					curr->next->sizeOfValuesArray=2*capacity;
				}
				curr->next->values[numOfElements]=strdup(value);
				curr->next->numOfElementsInValuesArray=curr->next->numOfElementsInValuesArray+1;

				pthread_mutex_unlock(&(targetBucket->bucketLock));

				free(keyValue->key);
				free(keyValue->values[0]);
				free(keyValue->values);
				free(keyValue);
				return;
			}
			curr=curr->next;
		}
		//if we get to here, it means that the key doesn't exist in the bucket's linked list yet
		//we will need to create a new <key,pair> node and insert it to the end of linked list
		
		curr->next=keyValue;

		pthread_mutex_unlock(&(targetBucket->bucketLock));

		pthread_mutex_lock(&(targetPartition->partitionLock));
		targetPartition->numKeys=targetPartition->numKeys+1;
		pthread_mutex_unlock(&(targetPartition->partitionLock));
		
		return;
	}
}


/* 		Takes in an integer i and convert the hashTable for partition[i] to a linear array, then return the array of KeyValuePair
 *
 **/
KeyValuePair** convertPartitionHashTableToArray(int partitionNumber)
{
	int i, arrayCounter=0;
	Partition* targetPartition=partitionArray[partitionNumber];
	HashTableBucket **hashtable=targetPartition->hashTable;

	KeyValuePair** array=malloc((targetPartition->numKeys)*sizeof(KeyValuePair*));



	for(i=0;i<SIZE_HASHTABLE;i++)
	{
		HashTableBucket *currentBucket=hashtable[i];
		KeyValueNode *curr=currentBucket->head;
		if(curr==NULL)
		{

			pthread_mutex_destroy(&(currentBucket->bucketLock));
			free(currentBucket);
			continue;
		}
		while(curr!=NULL)
		{
			KeyValuePair* keyValue=malloc(sizeof(KeyValuePair));
			// keyValue->key=strdup(curr->key);

			keyValue->key=curr->key;
			keyValue->values=curr->values;

			keyValue->numOfElementsInValuesArray=curr->numOfElementsInValuesArray;

			array[arrayCounter]=keyValue;
			arrayCounter++;

			KeyValueNode *temp=curr;
			curr=curr->next;
			// free(temp->key);
			// free(temp->values);
			free(temp);
		}

		pthread_mutex_destroy(&(currentBucket->bucketLock));
		free(currentBucket);
	}
	free(targetPartition->hashTable);

	// printf("arrayCounter is %d     targetPartition->numKeys is %d\n",arrayCounter,targetPartition->numKeys);
	// fflush(stdout);
	assert(arrayCounter==targetPartition->numKeys);    
	return array;
}


int compare(const void *keyValue1, const void *keyValue2)
{
	//comparator function takes in pointer to compared values, in this case, pointer to pointer of struct keyValue
	KeyValuePair* keyValuePair1=*((KeyValuePair**)keyValue1);
	KeyValuePair* keyValuePair2=*((KeyValuePair**)keyValue2);
	// printf("keyValuePair1->key: %s,  keyValuePair2->key: %s",keyValuePair1->key,keyValuePair2->key);
	// fflush(stdout);
	return strcmp(keyValuePair1->key,keyValuePair2->key);
}



void* SortPartitionsAndReduce(void* numPartition)
{
	int i;
	KeyValuePair** array=NULL;
	int partitionNumber=*((int*)numPartition);

	Partition* targetPartition=partitionArray[partitionNumber];
	//if current partition is empty, kill reduce thread
	if(targetPartition->numKeys==0)
	{
		pthread_exit(0);
	}

	array=convertPartitionHashTableToArray(partitionNumber);


	qsort(array,targetPartition->numKeys,sizeof(KeyValuePair*),compare);

	targetPartition->sortedKeyValuePairArray=array;

	for(i=0;i<targetPartition->numKeys;i++)
	{
		KeyValuePair* currentKeyValuePair=array[i];
		char* currentKey=currentKeyValuePair->key;
		targetPartition->currentArrayCounter=i;
		targetPartition->valueCounterForCurrentKeyValuePair=0;
		_Reduce(currentKey,get_next,partitionNumber);

		free(currentKeyValuePair->key);
		// for(j=0;j<currentKeyValuePair->numOfElementsInValuesArray;j++)
		// {
		// 	free(currentKeyValuePair->values[j]);
		// }
		free(currentKeyValuePair->values[(targetPartition->valueCounterForCurrentKeyValuePair)-1]);
		free(currentKeyValuePair->values);
		free(currentKeyValuePair);
	}

	free(targetPartition->sortedKeyValuePairArray);

	pthread_exit(0);
}



void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition)
{
	int i,j;
	pthread_t mappers[num_mappers];
	pthread_t reducers[num_reducers];
	int** partitionNumber=NULL;

	if(argc==1)
	{
		printf("Bad argument. Usage: ./mapreduce <filename_0> <filename_1> <filename_2> ... \n");
		exit(1);
	}
	numFiles=argc-1;
	numOfFilesLeft=numFiles;
	numOfPartition=num_reducers;
	_Map=map;
	_Reduce=reduce;
	_Partition=partition;

	//initialize lock for filetable
	pthread_mutex_init(&filetableLock,NULL);


	partitionNumber=malloc(num_reducers*sizeof(int*));

	//create partition arrays
	/**
	*   Design
	*   _________________________________________________________________________
	*   |           |           |           |           |           |           |
	*   | [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]| [  |  |  ]|
	*   |           |           |           |           |           |           |
	*   |           |           |           |           |           |           |
	*   |___________|___________|___________|___________|___________|___________|
	*        P1           P2         P3          P4           P5          P6
	*/ 

	partitionArray=malloc(num_reducers*sizeof(Partition*));

	//initialize each partition in the partition array
	for(i=0;i<num_reducers;i++)
	{
		partitionArray[i]=malloc(sizeof(Partition));
		Partition *currentPartition=partitionArray[i];
		currentPartition->hashTable=malloc(SIZE_HASHTABLE*sizeof(HashTableBucket*));     
		currentPartition->numKeys=0;
		currentPartition->sortedKeyValuePairArray=NULL;
		currentPartition->currentArrayCounter=0;
		currentPartition->valueCounterForCurrentKeyValuePair=0;

		pthread_mutex_init(&(currentPartition->partitionLock),NULL);

		for(j=0;j<SIZE_HASHTABLE;j++)
		{
			currentPartition->hashTable[j]=malloc(sizeof(HashTableBucket));
			currentPartition->hashTable[j]->head=NULL;

			pthread_mutex_init(&(partitionArray[i]->hashTable[j]->bucketLock),NULL);
			
		}
	}																					  


	
	//copy argv arrays to files array so that I can modify the content in files array
	files=malloc((numFiles)*sizeof(char*));
	for(i=0;i<numFiles;i++)
	{
		files[i]=strdup(argv[i+1]);
		//strcpy(files[i],argv[i+1]);
	}


	for(i=0;i<num_mappers;i++)
	{
		Pthread_create(&mappers[i],NULL,GrabFileAndMap,NULL);
	}


	for(i=0;i<num_mappers;i++)
	{
		Pthread_join(mappers[i],NULL);
	}

	// At this point here in method, mappers have finish doing all the work
	free(files);


	//sort the keys in the partition and reduce
	for(i=0;i<num_reducers;i++)
	{
		partitionNumber[i]=malloc(sizeof(int));
		*(partitionNumber[i])=i;
		Pthread_create(&reducers[i],NULL,SortPartitionsAndReduce,(void*)partitionNumber[i]);
	}

	for(i=0;i<num_reducers;i++)
	{
		Pthread_join(reducers[i],NULL);
	}


	// At this point here in method, reducers have finish doing all the work
	
	for(i=0;i<num_reducers;i++)
	{
		pthread_mutex_destroy(&(partitionArray[i]->partitionLock));
		free(partitionArray[i]);
		free(partitionNumber[i]);
	}
	free(partitionArray);
	free(partitionNumber);



	pthread_mutex_destroy(&filetableLock);





}




//////////////////////////////////////The functions below are customizable to suit for the job in hand

//A simple application of Map Reduce in word counting for arbitrary number of files
//Feel free to change anything you like in the functions below to suit the job in hand

unsigned long MR_DefaultHashPartition(char *key, int num_partitions)
{
	unsigned long hash=5381;
	int c;
	while((c=*key++)!='\0')
		hash=hash*33 +c;
	return (hash%num_partitions);
}


void Map(char* file_name)
{
	FILE *fp=fopen(file_name,"r");
	assert(fp!=NULL);

	char* line=NULL;
	size_t size=0;
	while(getline(&line,&size,fp)!=-1)
	{
		//token, dummy equals to a line in the file
		char *token, *dummy=line;
		//while curr pointer doesn't reach the end of current line
		while((token=strsep(&dummy," \t\n\r"))!=NULL)
		{
			MR_Emit(token,"1");
		}
	}
	free(line);
	fclose(fp);
}

void Reduce(char *key, Getter get_next, int partition_number)
{
	int count=0;
	char* value;
	//iterate all the values that produced the same key
	while((value=get_next(key,partition_number))!=NULL)
		count++;
	printf("%s %d\n",key,count);
}

int main(int argc, char* argv[])
{
					//Num of mapper thread
					//		|
					//		|	Number of reducer thread
					//		|		   |
					//		↓		   ↓
	MR_Run(argc, argv, Map, 1, Reduce, 1, MR_DefaultHashPartition);
	return 0;
}

