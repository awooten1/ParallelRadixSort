/**
* Adaptation of MPI_radix sort for use with openMP
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// global constants definitions
#define b 32 // number of bits for integer
#define g 8 // group of bits for each scan
#define N b / g // number of passes
#define B (1 << g) // number of buckets, 2^g

// MPI tags constants, offset by max bucket to avoid collisions
#define COUNTS_TAG_NUM B + 1
#define PRINT_TAG_NUM COUNTS_TAG_NUM + 1
#define NUM_TAG PRINT_TAG_NUM + 1
#define NTHREADS 16

// https://github.com/ym720/p_radix_sort_mpi/blob/master/p_radix_sort_mpi/p_radix_sort.c

// structure encapsulating buckets with arrays of elements
typedef struct list List;
struct list {
	int* array;
	size_t length;
	size_t capacity;
};

// add item to a dynamic array encapsulated in a structure
int add_item(List* list, int item) {
	if (list->length >= list->capacity) {
		size_t new_capacity = list->capacity*2;
		int* temp = realloc(list->array, new_capacity*sizeof(int));
		if (!temp) {
			printf("ERROR: Could not realloc for size %d!\n", (int) new_capacity);
			return 0;
		}
		list->array = temp;
		list->capacity = new_capacity;
	}

	list->array[list->length++] = item;

	return 1;
}

void usage(char* message) {
	fprintf(stderr, "Incorrect usage! %s\n", message);
	fprintf(stderr, "Usage: mpiexec -n [processes] p_radix_sort [f] [n] [r]\n");
	fprintf(stderr, " [processes] - number of processes to use\n");
	fprintf(stderr, " [f] - input file to be sorted\n");
	fprintf(stderr, " [n] - number of elements in the file\n");
	fprintf(stderr, " [r] - print sorted results 0/1, 0 by default\n");
}

// print resulting array while gathering information from all processes
void print_array(const int P, int *a, int *n) {
	int i, p;
	for (i = 0; i < n[0]; i++) {
		printf("%d\n", a[i]);
	}
}

// Initialize array with numbers read from a file
int init_array(char* file, const int begin, const int n, int *a) {

	// read n numbers from a file into array a starting at begin position
	int size = n-begin;
	int skip;
	int i;

	// open file in read-only mode and check for errors
	/*FILE *file_ptr;
	file_ptr = fopen(file, "r");
	if (file_ptr == NULL) {
		return EXIT_FAILURE;
	}*/

	// first skip to the begin position
	/*for (i = 0; i < begin; i++) {
		int s = fscanf(file_ptr, "%d", &skip);
	}*/

	// then read numbers into array a
	/*for (i = 0; i < n; i++) {
		int s = fscanf(file_ptr, "%d", &a[i]);
	}*/

	/* ------------------------------------------------------- */
	/*                     Generate Random                     */
	/* Instantiate array */
	for (i = begin; i < n+begin; ++i) {
		a[i-begin] = i;
	}

	/* Randomize the list of values */
	for (i = size - 1; i > 0; --i) {
		int w = rand()%i;
		int t = a[i];
		a[i] = a[w];
		a[w] = t;
	}
    /* ------------------------------------------------------- */

	return EXIT_SUCCESS;
}

// Compute j bits which appear k bits from the right in x
// Ex. to obtain rightmost bit of x call bits(x, 0, 1)
unsigned bits(unsigned x, int k, int j) {
	return (x >> k) & ~(~0 << j);
}

// Radix sort elements
// a - array of elements to be sorted
// buckets - array of buckets, each bucket pointing to array of elements
// n - number of elements to be sorted
int* radix_sort(int *a, List* buckets, int * n) {
	int count[B]; // array of counts per bucket for all processes
	int l_count[B]; // array of local process counts per bucket
	int p_sum[B]; // array of prefix sums
	int i, j, p, pass;

	for (pass = 0; pass < N; pass++) { // each pass

		// init counts arrays
		for (j = 0; j < B; j++) {
			count[j] = 0;
			l_count[j] = 0;
			buckets[j].length = 0;
		}

		// count items per bucket
		for (i = 0; i < *n; i++) {
			unsigned int idx = bits(a[i], pass*g, g);
			count[idx]++;
			l_count[idx]++;
			add_item(&buckets[idx], a[i]);
			/*if (!add_item(&buckets[idx], a[i])) {
				return NULL;
			}*/
		}

		// calculate new size based on counts
		int new_size = 0;
		for (j = 0; j < B; j++) {
			p_sum[j] = new_size;
			new_size += count[j];
		}

		// reallocate array if newly calculated size is larger
		if (new_size > *n) {
			int* temp = realloc(a, new_size*sizeof(int));
			if (!a) {
				printf("ERROR: Could not realloc for size %d!\n", new_size);
				return NULL;
			}
			// reassign pointer back to original
			a = temp;
		}

		// position keys in final array
		#pragma omp parallel for num_threads(NTHREADS)
		for (j = 0; j < B; j++) {

			// get bucket count
			int b_count = count[j];
			if (b_count > 0) {
	
				// point to an index in array where to insert received keys
				int *dest = &a[p_sum[j]];
				memcpy(dest, &buckets[j].array[0], b_count*sizeof(int));
			}
		}

		// update new size
		*n = new_size;
	}

	return a;
}

int main(int argc, char** argv)
{
	int size, i, j;
	int print_results = 0;
	size = 1;
	
	// check for correct number of arguments
	if (argc < 3)
	{
		usage("Not enough arguments!");
		return EXIT_FAILURE;
	} 
	else if (argc > 3) {
    	print_results = atoi(argv[3]);
	}

	// initialize vars and allocate memory
	int n_total = atoi(argv[2]);
	int n = n_total/size;
	if (n < 1) {
		usage("Number of elements must be >= number of processes!");
		return EXIT_FAILURE;
	}

	int remainder = B % size; // in case number of buckets is not divisible
	if (remainder > 0) {
		usage("Number of buckets must be divisible by number of processes\n");
		return EXIT_FAILURE;
	}

	const int s = 0;
	int* a = malloc(sizeof(int) * n);

	int b_capacity = n / B;
	if (b_capacity < B) {
		b_capacity = B;
	}
	List* buckets = malloc(B*sizeof(List));
	for (j = 0; j < B; j++) {
		buckets[j].array = malloc(b_capacity*sizeof(int));
		buckets[j].capacity = B;
	}

	// initialize local array
	if (init_array(argv[1], s, n, &a[0]) != EXIT_SUCCESS) {
		printf("File %s could not be opened!\n", argv[1]);
		return EXIT_FAILURE;
	}

	// then run the sorting algorithm
	a = radix_sort(&a[0], buckets, &n);
	
	if (a == NULL) {
		printf("ERROR: Sort failed, exiting ...\n");
		return EXIT_FAILURE;
	}

	// store number of items per each process after the sort
	int* p_n = malloc(size*sizeof(int));

	// first store our own number
	p_n[0] = n;
  
	// print results
	if (print_results) {
		print_array(size, &a[0], p_n);
	}

	// release memory allocated resources
	for (j = 0; j < B; j++) {
		free(buckets[j].array);
	}
	free(buckets);
	free(a);
	free(p_n);
	
	return 0;
}