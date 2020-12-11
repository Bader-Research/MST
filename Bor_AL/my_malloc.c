#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "simple.h"

#if 0
void * my_malloc(size_t size)
{
	int fd;
	void * p=NULL;
	
	if ( (fd = open("/dev/zero", O_RDWR)) < 0)
		printf("open error\n");
	if ( (p = mmap((void *)0, size, PROT_READ | PROT_WRITE,MAP_SHARED,fd,0))== (caddr_t)-1)
	{
		printf("mmap error\n");
	}
  	close(fd);
	return p; 
}

#endif

void ** my_mem;
void ** rem_head;
int * rem_size;
int * total_size;

void init_mem(size_t size,THREADED)
{
	int fd;
	void * p=NULL;
	
	if ( (fd = open("/dev/zero", O_RDWR)) < 0)
		printf("open error\n");
	if ( (p = mmap((void *)0, size, PROT_READ | PROT_WRITE,MAP_SHARED,fd,0))== (caddr_t)-1)
	{
		printf("mmap error\n");
	}
  	close(fd);
	my_mem = node_malloc(sizeof(void*)*THREADS,TH);
	rem_head = node_malloc(sizeof(void*)*THREADS,TH);
	rem_size = node_malloc(sizeof(int)*THREADS,TH);
	total_size = node_malloc(sizeof(int)*THREADS,TH);
	total_size[MYTHREAD]=size;
	rem_size[MYTHREAD]=size;
	rem_head[MYTHREAD]=p;
	my_mem[MYTHREAD]=p;
}

void* my_malloc(size_t size,THREADED)
{
	void * p ;
	if(size>rem_size[MYTHREAD]){
		printf("my_malloc:Not enough memory\n");
		return(NULL);
	}
	p = rem_head[MYTHREAD];
	rem_head[MYTHREAD]+=size;
	rem_size[MYTHREAD]-=size;
	return (p);
}

void clear_mem(size_t size,THREADED)
{
	munmap(my_mem[MYTHREAD],total_size[MYTHREAD]);
	node_Barrier();
	node_free(my_mem,TH);
	node_free(rem_head,TH);
	node_free(rem_size,TH);
	node_free(total_size,TH);
}
