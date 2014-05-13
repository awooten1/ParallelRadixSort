from mpi4py import MPI
from time import sleep
from radix_sort import radix_sort
from random import randrange
from merge import mergeSortedLists

def init_array(size_of_data):
	data_array = []
	for i in range(size_of_data + 1):
		data_array.append(None)
	return data_array

def compress(sorted_data):
	return [x for x in sorted_data if x != None]


def random_data(maxint, datasize):
	return [randrange(1,maxint) for i in range(datasize)]

def read_int():
	data_file = open("input.dat", "r")
	scrubbed_data = []
	max_int = 0
	for line in data_file:
		for token in line.strip().split(" "):
			int_token = int(token)
			scrubbed_data.append(int_token)
	return scrubbed_data


if __name__ == '__main__':


	#Initialize OpenMPI
	comm = MPI.COMM_WORLD
	rank = comm.Get_rank()
	numprocs = comm.Get_size()



	if rank == 0:
		MAX_SIZE = 1000
		INPUT_SIZE = 1000
		data = random_data(MAX_SIZE, INPUT_SIZE)

		for i in range(1,numprocs):
			interval = len(data) / (numprocs - 1)
			start = ((i - 1) * interval)
			end = start + interval
			comm.send(data[start:end], dest=i, tag=1)



		
		#Receive the sorted section of the data from the worker node
		results = []
		for i in range(1, numprocs):
			results += [comm.recv(source=i, tag=0)]

		#######Aggregate all of the results##############


		#Merge the results from the workers. Runs in O(lg(n))
		sorted_nums = mergeSortedLists(results[0],results[1])
		del results[0]
		del results[0]
		while len(results) != 0:
			#merge the first two lists
			sorted_nums += mergeSortedLists(sorted_nums, results[0])
			del results[0]

		print sorted_nums
		
		


	else:
		data = comm.recv(source=0, tag=1)
		#sort by current digit
		for i in range(len(str(1000)) - 1):
			data = radix_sort(data, i, 10)

		comm.send(data, dest=0, tag=0)
