#include "simple.h"
#include "types.h"
#include "values.h"

#define LOCATION 0
#define thread_id MYTHREAD
#define barrier_wait(A) node_Barrier()
#define getclock(A,B) ;
#define MAXELE MAXINT


extern int * D;
TYPE MAX_VAL, MAX_VAL_m1;

inline int mylt(TYPE  a, TYPE  b)
{
	if((D[a->v1]<D[b->v1]) || ((D[a->v1]==D[b->v1]) && (D[a->v2]<D[b->v2])))
		return 1;
	return 0;
#if 0
	return(a<b);
#endif
}

inline int myle(TYPE  a, TYPE b)
{
	if((D[a->v1]<D[b->v1]) || ((D[a->v1]==D[b->v1]) && (D[a->v2]<=D[b->v2])))
		return 1;
	return 0;
#if 0
	return(a<=b);
#endif
}

inline int myeq(TYPE  a, TYPE b)
{
	if((D[a->v1]==D[b->v1]) && D[a->v2]==D[b->v2]) return 1;
	return 0;
#if 0
	return (a==b);
#endif
}

inline int mygt(TYPE a, TYPE b)
{
	if((D[a->v1]>D[b->v1]) || ((D[a->v1]==D[b->v1]) && (D[a->v2]>D[b->v2])))
		return 1;
	return 0;
#if 0
	return(a>b);
#endif
}

#if 0
#define mylt(a,b) ((D[(a)->v1]<D[(b)->v1]) || ((D[(a)->v1]==D[(b)->v1]) && (D[(a)->v2]<D[(b)->v2])))? 1:0
#define myle(a,b) ((D[(a)->v1]<D[(b)->v1]) || ((D[(a)->v1]==D[(b)->v1]) && (D[(a)->v2]<=D[(b)->v2])))?1:0
#define myeq(a,b) ((D[(a)->v1]==D[(b)->v1]) && D[(a)->v2]==D[(b)->v2]) ? 1:0
#define mygt(a,b) ((D[(a)->v1]>D[(b)->v1]) || ((D[(a)->v1]==D[(b)->v1]) && (D[(a)->v2]>D[(b)->v2])))?1:0
#define MYMIN(a,b) ((D[(a)->v1]<D[(b)->v1]) || ((D[(a)->v1]==D[(b)->v1]) && (D[(a)->v2]<=D[(b)->v2])))?(a):(b)
#endif

void init_sort(TYPE max_dummy, TYPE max_dummy_m1)
{
	MAX_VAL = max_dummy;
	MAX_VAL_m1 = max_dummy_m1;
}

#if 0
void put_int(TYPE a, int i)
{
	if(a) a->w = i;
	else {
			a = malloc(sizeof(edge_t));
			a->w = i;
		}
}

int get_int(TYPE a)
{
	return a->w;
}
#endif


#define put_int(a,i) a=(i)
#define get_int(a) (int)(a)

inline TYPE MYMIN (TYPE a, TYPE b)
{
	if((D[a->v1]<D[b->v1]) || ((D[a->v1]==D[b->v1]) && (D[a->v2]<=D[b->v2])))
		return a;
	return b;
}

void master_regular_integer_sort(int size,int M,int p,int samples,TYPE *Buffer1,
			   TYPE *Buffer2,TYPE **out_array_ptr,THREADED)			   
{
   
register int i,j,k,rem,tot,tot1,tot2,done,last1,last2,c_start,t_start,
col_size;

int adjustments, times, rems, reps, counts,log_M,l,r,begin, length,s,cpu,
entries,pp,v,t,stride,split_count,values_m1,offset1,offset2,
radix,radix_m1,log_value_range,digits,values,shift1,shift2,COL_SIZE1,
COL_SIZE2,MOD,SLAVES;

TYPE c_val, t_val, split_val;

TYPE *temp_ptr,*dest_ptr,
     *start_ptr,*finish_ptr,*s_ptr,
     *s1_ptr,*s2_ptr,*t1_ptr,*s3_ptr,*s4_ptr,*t3_ptr,*t4_ptr,
     *t2_ptr, *buffer1_ptr, *buffer2_ptr, **Source,
     **source_ptr, *Sample1, *Sample2, *sample_ptr, *Aux,v1,v2,v3,v4,
     *Count,*Count1,*Count2,*t_ptr,*c_ptr,*Next,*Offset,dummy;

int *Number, *Counts, *counts_ptr;
record_t *root_ptr,*tree_ptr, *Tree, *TempT;
split_t *Splitters, *split_ptr;

hrtime_t T_start, T_end;
double interval;

int d_b=0;
T_start = gethrtime();

SLAVES = THREADS - 1;

Tree = (record_t *) node_malloc((p + THREADS/2)*sizeof(record_t),TH);
TempT = (record_t *) node_malloc((p + THREADS/2)*sizeof(record_t),TH);
Source = (TYPE **) node_malloc((p + THREADS/2)*sizeof(TYPE *),TH);
Sample1 = (TYPE *) node_malloc(THREADS*samples*sizeof(TYPE),TH);
Sample2 = (TYPE *) node_malloc(THREADS*samples*sizeof(TYPE),TH);
Splitters = (split_t *) node_malloc(THREADS*sizeof(split_t),TH);
Counts = (int *) node_malloc(THREADS*THREADS*sizeof(int),TH);
Number = (int *) node_malloc(THREADS*sizeof(int),TH);
Aux = (TYPE *) node_malloc(M*sizeof(TYPE),TH);

#if (SORTTYPE == 1)

col_size = size/THREADS;
MOD = size % THREADS;
if (MOD)
  col_size++;

rems = col_size % M;
reps = (col_size - rems)/M; 

if ((reps) && (rems <= (col_size/2)))
  {rems += M;
   reps--;
   Aux = (TYPE *) malloc(rems*sizeof(TYPE));
   assert_malloc(Aux);
  }
else
  {Aux = (TYPE *) malloc(M*sizeof(TYPE));
   assert_malloc(Aux);
  }

if (MOD)  
  {if (thread_id == (THREADS - 1))
     for (i=0;i<(THREADS - MOD);i++)
       *(Buffer1 + size + i) = MAX_VAL;
   COL_SIZE1 = col_size;
   COL_SIZE2 = col_size - (THREADS - MOD);
  }
else
  COL_SIZE2 = COL_SIZE1 = col_size;

offset1 = COL_SIZE1 + 2*((int) (COL_SIZE1/M + 1)) + 2;
start_ptr = buffer1_ptr = Buffer1 + thread_id*COL_SIZE1 + reps*M;
dest_ptr =  Buffer2 + thread_id*offset1 + reps*(M + 2);

log_M = ((int) floor((log((double) M)/log((double) 2))+0.1)); 

if (rems)       
  {if (rems > 2)
     {t1_ptr = (s1_ptr = start_ptr) + rems;
      if (rems & 1)
	t1_ptr--;
      finish_ptr = buffer2_ptr = Aux;
      while (s1_ptr < t1_ptr) 
	{if (myle((*s1_ptr),*(s1_ptr+1)))
	   {*(finish_ptr++) = *(s1_ptr++);
	    *(finish_ptr++) = *(s1_ptr++);
	   }
	 else 
	   {*(finish_ptr++) = *(s1_ptr+1);
	    *(finish_ptr++) = *s1_ptr;
	    s1_ptr += 2;
	   }
	} 
      last1 = last2 = 2;
      if (rems & 1)
	{*finish_ptr = *s1_ptr;
	 last2 = 1;
	}
      times = (int) ceil(log((double) rems)/log((double) 2)); 
      temp_ptr = buffer1_ptr;
      buffer1_ptr = buffer2_ptr;
      buffer2_ptr = temp_ptr;
      tot = 2;
      tot1 = 1;
      tot2 = rems >> 1;
      for (i=2;i<times;i++)
	{tot <<= 1; 
	 tot1 <<= 1;
	 tot2 >>= 1;
	 t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + tot1) + tot1;
	 finish_ptr = buffer2_ptr;
	 for (j=1;j<tot2;j++)
	   {done = 0;
	    while (! done) 
	      {if (myle(*s1_ptr,*s2_ptr))
		 {*(finish_ptr++) = *(s1_ptr++);
		  if (s1_ptr == t1_ptr) 
		    {while (s2_ptr < t2_ptr)
		       *(finish_ptr++) = *(s2_ptr++);
		     done = 1;
		    }
		 }
	       else
		 {*(finish_ptr++) = *(s2_ptr++);
		  if (s2_ptr == t2_ptr) 
		    {while (s1_ptr < t1_ptr)
		       *(finish_ptr++) = *(s1_ptr++);
		     done = 1;
		    }
		 }
	      }
	    t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = t2_ptr) + tot1) + tot1;    
	   }
	 rem = (rems & (tot - 1));
	 if (! rem)
	   {done = 0;
	    while (! done) 
	      {if (myle((*s1_ptr), (*s2_ptr)))
		 {*(finish_ptr++) = *(s1_ptr++);
		  if (s1_ptr == t1_ptr) 
		    {while (s2_ptr < t2_ptr)
		     *(finish_ptr++) = *(s2_ptr++);
		     done = 1;
		    }
		 }
	       else
		 {*(finish_ptr++) = *(s2_ptr++);
		  if (s2_ptr == t2_ptr) 
		    {while (s1_ptr < t1_ptr)
		     *(finish_ptr++) = *(s1_ptr++);
		     done = 1;
		    }
		 }
	      }
	    last1 = last2 = tot;
	   }
	 else  
	   {if (rem <= tot1)
	      {for (k=0;k<=tot1;k++)
		 *(finish_ptr + k) = *(s1_ptr + k);
	       finish_ptr += tot1; 
	       t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = s1_ptr + tot1) 
							    + last1) + last2; 
	       done = 0;
	       while (! done) 
		 {if (myle((*s1_ptr),(*s2_ptr)))
		    {*(finish_ptr++) = *(s1_ptr++);
		     if (s1_ptr == t1_ptr) 
		       {while (s2_ptr < t2_ptr)
			  *(finish_ptr++) = *(s2_ptr++);
			done = 1;
		       }
		    }
		  else
		    {*(finish_ptr++) = *(s2_ptr++);
		     if (s2_ptr == t2_ptr) 
		       {while (s1_ptr < t1_ptr)
			  *(finish_ptr++) = *(s1_ptr++);
			done = 1;
		       }
		    }
		 }
	       last2 += last1;
	       last1 = tot1;
	      }
	    else
	      {done = 0;
	       while (! done) 
		 {if (myle((*s1_ptr),(*s2_ptr)))
		    {*(finish_ptr++) = *(s1_ptr++);
		     if (s1_ptr == t1_ptr) 
		       {while (s2_ptr < t2_ptr)
			  *(finish_ptr++) = *(s2_ptr++);
			done = 1;
		       }
		    }
		  else
		    {*(finish_ptr++) = *(s2_ptr++);
		     if (s2_ptr == t2_ptr) 
		       {while (s1_ptr < t1_ptr)
			  *(finish_ptr++) = *(s1_ptr++);
			done = 1;
		       }
		    }
		 }
	       t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = t2_ptr) + last1) + last2;    
	       done = 0;
	       while (! done) 
		 {if (myle((*s1_ptr),(*s2_ptr)))
		    {*(finish_ptr++) = *(s1_ptr++);
		     if (s1_ptr == t1_ptr) 
		       {while (s2_ptr < t2_ptr)
			  *(finish_ptr++) = *(s2_ptr++);
			done = 1;
		       }
		    }
		  else
		    {*(finish_ptr++) = *(s2_ptr++);
		     if (s2_ptr == t2_ptr) 
		       {while (s1_ptr < t1_ptr)
			  *(finish_ptr++) = *(s1_ptr++);
			done = 1;
		       }
		    }
		 }
	       last2 += last1;
	       last1 = tot;
	      }
	   }
	 temp_ptr = buffer1_ptr;
	 buffer1_ptr = buffer2_ptr;
	 buffer2_ptr = temp_ptr;
	} 
      t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + last1) + last2;    
      finish_ptr = dest_ptr;
      *(finish_ptr++)=rems;  /* put_int*/
      done = 0;
      while (! done) 
	{if (myle((*s1_ptr), (*s2_ptr)))
	   {*(finish_ptr++) = *(s1_ptr++);
	    if (s1_ptr == t1_ptr) 
	      {while (s2_ptr < t2_ptr)
		 *(finish_ptr++) = *(s2_ptr++);
		 done = 1;
	      }
	   }
	 else
	   {*(finish_ptr++) = *(s2_ptr++);
	    if (s2_ptr == t2_ptr) 
	      {while (s1_ptr < t1_ptr)
		 *(finish_ptr++) = *(s1_ptr++);
	       done = 1;
	      } 
	   }
	}
      *finish_ptr = MAX_VAL;
     }
   else
     {if (rems == 1)
	{*(dest_ptr + 1) = *start_ptr;
	 put_int(*(dest_ptr) , 1);   
	 *(dest_ptr + 2) = MAX_VAL;
	} 
      else  
	{if (rems == 2)
	   {if (myle(*start_ptr ,*(start_ptr+1)))
	      {*(dest_ptr + 2) = *(start_ptr+1);
	       *(dest_ptr + 1) = *start_ptr; 
	       put_int(*(dest_ptr), 2);   
	       *(dest_ptr + 3) = MAX_VAL; 
	      } 
	    else 
	      {*(dest_ptr + 1) = *(start_ptr+1);
	       *(dest_ptr + 2) = *start_ptr; 
	       put_int(*(dest_ptr) ,2);   
	       *(dest_ptr + 3) = MAX_VAL;   
	      } 
	   }
	}
     }    
  }   
     
for (r=0;r<reps;r++)       
  {t1_ptr = (s1_ptr = buffer1_ptr = (start_ptr -= M)) + M;
   finish_ptr = buffer2_ptr = Aux;
   while (s1_ptr < t1_ptr) 
     {if (myle((*s1_ptr),(*(s1_ptr+1))))
	{*(finish_ptr++) = *(s1_ptr++);
	 *(finish_ptr++) = *(s1_ptr++);
	}
      else  
	{*(finish_ptr++) = *(s1_ptr+1);
	 *(finish_ptr++) = *s1_ptr;
	 s1_ptr += 2;
	}
     } 
   times = log_M; 
   temp_ptr = buffer1_ptr;
   buffer1_ptr = buffer2_ptr;
   buffer2_ptr = temp_ptr;
   tot = 2;
   tot1 = 1;
   tot2 = M >> 1;
   for (i=2;i<times;i++)
     {tot <<= 1; 
      tot1 <<= 1;
      tot2 >>= 1;
      t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + tot1) + tot1;
#if 0   
     sleep(MYTHREAD);
   	  printf("\n s1_ptr :");
      while(s1_ptr != t1_ptr)
	  {
	  	if(*s1_ptr> 1000)
	  		printf(" (%d,%d) ", (*s1_ptr)->v1,(*s1_ptr)->v2);
		else printf ("%d ", *s1_ptr);
		s1_ptr++;
	  }
	  printf("\n");
	  printf("s2_ptr:");
   	  while(s2_ptr != t2_ptr)
	  {
	  	if(*s2_ptr> 1000)
	  		printf(" (%d,%d) ", (*s2_ptr)->v1,(*s2_ptr)->v2);
		else printf ("%d ", *s2_ptr);
		s2_ptr++;
	  }
	  node_Barrier();
	  t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + tot1) + tot1;	
#endif
	  
      finish_ptr = buffer2_ptr;
      for (j=0;j<tot2;j++)
	{done = 0;
	 while (! done) 
	   {if (myle((*s1_ptr), (*s2_ptr)))
	      {*(finish_ptr++) = *(s1_ptr++);
	       if (s1_ptr == t1_ptr) 
		 {while (s2_ptr < t2_ptr)
		    *(finish_ptr++) = *(s2_ptr++);
		  done = 1;
		 }
	      }
	    else
	      {*(finish_ptr++) = *(s2_ptr++);
	       if (s2_ptr == t2_ptr) 
		 {while (s1_ptr < t1_ptr)
		    *(finish_ptr++) = *(s1_ptr++);
		  done = 1;
		 }
	      }
	   }
	 t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = t2_ptr) + tot1) + tot1;    
	}
      temp_ptr = buffer1_ptr;
      buffer1_ptr = buffer2_ptr;
      buffer2_ptr = temp_ptr;
     } 
   t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + M/2) + M/2;    
   finish_ptr = dest_ptr -= (M + 2);
   put_int(*(finish_ptr++) , M); 
   done = 0;
   while (! done) 
     {if (myle((*s1_ptr) , (*s2_ptr)))
	{*(finish_ptr++) = *(s1_ptr++);
	 if (s1_ptr == t1_ptr) 
	   {while (s2_ptr < t2_ptr)
	      *(finish_ptr++) = *(s2_ptr++);
	    done = 1;
	   }
	}
      else
	{*(finish_ptr++) = *(s2_ptr++);
	 if (s2_ptr == t2_ptr) 
	   {while (s1_ptr < t1_ptr)
	      *(finish_ptr++) = *(s1_ptr++);
	    done = 1;
	   } 
	}
     }
   *finish_ptr = MAX_VAL;
  }

free(Aux);

barrier_wait(t_barrier);  
getclock(TIMEOFDAY,its1_ptr);   
   
#if 0
	on_one {
		printf("Before sequential merge:\n");
		for(i=0;i<size;i++){
			if(Buffer2[i]<10000) printf(" %d ", Buffer2[i]);
			else printf(" (%d,%d) ", D[Buffer2[i]->v1],D[Buffer2[i]->v2]);
			if((i+1)%5 == 0) printf("\n");
		}
		printf("\n");
	}
	node_Barrier();
#endif


if (col_size > rems)
  {
   Tree = (record_t *) malloc((p + THREADS/2)*sizeof(record_t));

   assert_malloc(Tree); 

   TempT = (record_t *) malloc((p + THREADS/2)*sizeof(record_t));

   assert_malloc(TempT); 
   
   Source = (TYPE **) malloc((p + THREADS/2)*sizeof(TYPE *));

   assert_malloc(Source); 
   
   start_ptr = Buffer2 + thread_id*offset1;
   dest_ptr = Buffer1 + thread_id*offset1;

   adjustments = 0;
   times = ((int) ceil(log(((double) col_size)/((double) M))/
	     log((double) p))); 
   
   r = ((int) ceil (((double) col_size)/((double) M)));
   if (rems > M)
     {r--;
      if (r == ((int) pow(((double) p),((double) (times-1)))))
	times--;
     }
   s = r - ((int) pow(((double) p),((double) (times-1))));
   rems = s % (p-1);
   reps = (s - rems)/(p - 1);
   buffer1_ptr = start_ptr - 1;
   finish_ptr = dest_ptr;
   for (i=0;i<(r-1);i++)
     {temp_ptr = (buffer1_ptr += (M+2));
      while (myeq(*(--temp_ptr), MAX_VAL))
	{adjustments++;
	 *temp_ptr = MAX_VAL_m1;
	}
     }
   temp_ptr = start_ptr + col_size + 2*r - 1;
   while (myeq(*(--temp_ptr), MAX_VAL))
     {adjustments++;
      *temp_ptr = MAX_VAL_m1;
     }
   if (rems || reps)
     times--;
   entries = 0;
   counts = 0;
   if (rems)
     {rems ++;
      pp =  (int) pow(2.0,ceil(log((double) rems)/log(2.0))); 
      buffer1_ptr = start_ptr + (r - rems - 1)*(M + 2); 
      if (pp > 2)
	{for (i=0;i<rems;i++)
	   {entries += get_int((*(buffer1_ptr += (M + 2))));
	    *(Source + i) = (buffer1_ptr + 1);
	   }
	 dummy = MAX_VAL;
	 counts += entries;
	 for (i=rems;i<pp;i++)
	   *(Source + i) = &dummy;
	 source_ptr = Source;
	 j = - 2;
	 for (i=pp/2;i<pp;i++)
	   {if (mygt((**(source_ptr + 1)) , (**(source_ptr))))
	      {(Tree + i)->val = (**(source_ptr + 1));
	       (Tree + i)->start = (j +=  2) + 1;
	       (TempT + i)->val = (**(source_ptr));
	       (TempT + i)->start = j;
	       source_ptr += 2;
	      }
	    else  
	      {(Tree + i)->val = (**(source_ptr));
	       (Tree + i)->start = (j +=  2);
	       (TempT + i)->val = (**(source_ptr + 1));
	       (TempT + i)->start = j + 1;
	       source_ptr += 2;
	      }
	   }
	 for (i=(pp/2 - 1);i>0;i--)
	   {k = (j = (i << 1)) + 1;
	    if (mygt(((TempT + k)->val) , ((TempT + j)->val)))
	      {(Tree + i)->val = (TempT + k)->val;
	       (Tree + i)->start = (TempT + k)->start;
	       (TempT + i)->val = (TempT + j)->val;
	       (TempT + i)->start = (TempT + j)->start;
	      }
	    else
	      {(Tree + i)->val = (TempT + j)->val;
	       (Tree + i)->start = (TempT + j)->start;
	       (TempT + i)->val = (TempT + k)->val;
	       (TempT + i)->start = (TempT + k)->start;
	      }
	   }
	 put_int(*(finish_ptr++) , entries); 
	 temp_ptr = finish_ptr + entries;     
	 *(finish_ptr++) = (TempT + 1)->val; 
	 c_start = (TempT + 1)->start;
	 root_ptr = Tree + 1;
	 while (finish_ptr < temp_ptr)
	   {c_val = (*(++(*(Source + c_start))));  
	    i = c_start + pp;
	    while (i > 3)
	      {if (mygt(c_val, ((Tree + (i >>= 1))->val)))
		 {t_val = c_val;
		  t_start = c_start;
		  c_val = (tree_ptr = (Tree + i))->val;
		  c_start = tree_ptr->start;
		  tree_ptr->val = t_val;
		  tree_ptr->start = t_start;
		 }
	      }
	    if (mygt(c_val , (root_ptr->val)))
	      {t_start = c_start;
	       c_start = root_ptr->start;
	       root_ptr->start = t_start;
	       *(finish_ptr++) = root_ptr->val;
	       root_ptr->val = c_val;
	      }
	    else
	      *(finish_ptr++) = c_val;
#if 0		  
		 printf(" *finish_ptr -> %d\n", (*(finish_ptr-1))->v1);
#endif
		   
	   }
	 *(finish_ptr++) = MAX_VAL;
	 printf(" *finish_ptr -> %d\n", (*(finish_ptr-1))->v1); 
	}
      else
	{entries += get_int((*(buffer1_ptr += (M + 2))));
	 s1_ptr = buffer1_ptr + 1;
	 entries += get_int((*(buffer1_ptr += (M + 2))));
	 s2_ptr = buffer1_ptr + 1;
	 counts += entries;
	 put_int(*(finish_ptr++) , entries);  
	 temp_ptr = finish_ptr + entries;     
	 while (finish_ptr < temp_ptr) 
	   {if (myle(*s1_ptr , *s2_ptr))
	      *(finish_ptr++) = *(s1_ptr++);
	    else 
	      *(finish_ptr++) = *(s2_ptr++);
#if 0		  
		 printf(" *finish_ptr -> %d\n", (*(finish_ptr-1))->v1);
#endif
		  
	   }
	 *(finish_ptr++) = MAX_VAL;
#if 0	 
	 printf(" *finish_ptr -> %d\n", (*(finish_ptr-1))->v1); 
#endif	 
	}
     }
   buffer1_ptr = start_ptr + (r - rems - (reps*p) - 1)*(M + 2);
   for (t=0;t<reps;t++)
     {entries = 0;
      for (i=0;i<p;i++)
	{entries += get_int((*(buffer1_ptr += (M + 2))));
	 *(Source + i) = (buffer1_ptr + 1);
	}
      counts += entries;
      source_ptr = Source;
      j = - 2;
      for (i=p/2;i<p;i++)
	{if (mygt((**(source_ptr + 1)) , (**(source_ptr))))
	   {(Tree + i)->val = (**(source_ptr + 1));
	    (Tree + i)->start = (j +=  2) + 1;
	    (TempT + i)->val = (**(source_ptr));
	    (TempT + i)->start = j;
	    source_ptr += 2;
	   }
	 else  
	   {(Tree + i)->val = (**(source_ptr));
	    (Tree + i)->start = (j +=  2);
	    (TempT + i)->val = (**(source_ptr + 1));
	    (TempT + i)->start = j + 1;
	    source_ptr += 2;
	   }
	}
      for (i=(p/2 - 1);i>0;i--)
	{k = (j = (i << 1)) + 1;
	 if (mygt(((TempT + k)->val) ,((TempT + j)->val)))
	   {(Tree + i)->val = (TempT + k)->val;
	    (Tree + i)->start = (TempT + k)->start;
	    (TempT + i)->val = (TempT + j)->val;
	    (TempT + i)->start = (TempT + j)->start;
	   }
	 else
	   {(Tree + i)->val = (TempT + j)->val;
	    (Tree + i)->start = (TempT + j)->start;
	    (TempT + i)->val = (TempT + k)->val;
	    (TempT + i)->start = (TempT + k)->start;
	   }
	}
      put_int(*(finish_ptr++) , entries); 
      temp_ptr = finish_ptr + entries;     
      *(finish_ptr++) = (TempT + 1)->val;
      c_start = (TempT + 1)->start;
      root_ptr = Tree + 1;
      while (finish_ptr < temp_ptr)
	{c_val = (*(++(*(Source + c_start))));  
	 i = c_start + p;
	 while (i > 3)
	   {if (mygt(c_val , ((Tree + (i >>= 1))->val)))
	      {t_val = c_val;
	       t_start = c_start;
	       c_val = (tree_ptr = (Tree + i))->val;
	       c_start = tree_ptr->start;
	       tree_ptr->val = t_val;
	       tree_ptr->start = t_start;
	      }
	   }
	 if (mygt(c_val , (root_ptr->val)))
	   {t_start = c_start;
	    c_start = root_ptr->start;
	    root_ptr->start = t_start;
	    *(finish_ptr++) = root_ptr->val;
	    root_ptr->val = c_val;
	   }
	 else
	   *(finish_ptr++) = c_val;  
	}
      *(finish_ptr++) = MAX_VAL;
     }
   if (counts > col_size/2)    
     {s1_ptr = buffer1_ptr = start_ptr;
      temp_ptr = start_ptr + (r - rems - (reps*p))*(M + 2);
      while (s1_ptr < temp_ptr)
	*(finish_ptr++) = *(s1_ptr++);
      if (times)
	{temp_ptr = start_ptr;
	 start_ptr = dest_ptr;
	 dest_ptr = temp_ptr;
	}
     }
   else
     {temp_ptr = finish_ptr;
      s1_ptr = dest_ptr;
      finish_ptr = start_ptr + (r - rems - (reps*p))*(M + 2);  
      while (s1_ptr < temp_ptr)
	*(finish_ptr++) = *(s1_ptr++);
     }
   r = ((int) pow(((double) p),((double) (times))));
   for (t=0;t<times;t++)
     {buffer1_ptr = start_ptr;
      finish_ptr = dest_ptr;
      r = (int) (r/p);
      for (s=0;s<r;s++)
	{entries = 0;
	 for (i=0;i<p;i++)
	   {
	    j = get_int(*(buffer1_ptr));
		entries += j;
	    *(Source + i) = (buffer1_ptr + 1);
	    buffer1_ptr += (j + 2);
	   }
	 source_ptr = Source;
	 j = - 2;
	 for (i=p/2;i<p;i++)
	   {if (mygt((**(source_ptr + 1)) , (**(source_ptr))))
	      {(Tree + i)->val = (**(source_ptr + 1));
	       (Tree + i)->start = (j +=  2) + 1;
	       (TempT + i)->val = (**(source_ptr));
	       (TempT + i)->start = j;
	       source_ptr += 2;
	      }
	    else  
	      {(Tree + i)->val = (**(source_ptr));
	       (Tree + i)->start = (j +=  2);
	       (TempT + i)->val = (**(source_ptr + 1));
	       (TempT + i)->start = j + 1;
	       source_ptr += 2;
	      }
	   }
	 for (i=(p/2 - 1);i>0;i--)
	   {k = (j = (i << 1)) + 1;
	    if (mygt(((TempT + k)->val) , ((TempT + j)->val)))
	      {(Tree + i)->val = (TempT + k)->val;
	       (Tree + i)->start = (TempT + k)->start;
	       (TempT + i)->val = (TempT + j)->val;
	       (TempT + i)->start = (TempT + j)->start;
	      }
	    else
	      {(Tree + i)->val = (TempT + j)->val;
	       (Tree + i)->start = (TempT + j)->start;
	       (TempT + i)->val = (TempT + k)->val;
	       (TempT + i)->start = (TempT + k)->start;
	      }
	   }
	 put_int(*(finish_ptr++) , entries);
	 temp_ptr = finish_ptr + entries;
	 *(finish_ptr++) = (TempT + 1)->val; 
	 c_start = (TempT + 1)->start;
	 root_ptr = Tree + 1;
	 while (finish_ptr < temp_ptr)
	   {c_val = (*(++(*(Source + c_start))));  
	    i = c_start + p;
	    while (i > 3)
	      {if (mygt(c_val , ((Tree + (i >>= 1))->val)))
		 {t_val = c_val;
		  t_start = c_start;
		  c_val = (tree_ptr = (Tree + i))->val;
		  c_start = tree_ptr->start;
		  tree_ptr->val = t_val;
		  tree_ptr->start = t_start;
		 }
	      }
	    if (mygt(c_val ,(root_ptr->val)))
	      {t_start = c_start;
	       c_start = root_ptr->start;
	       root_ptr->start = t_start;
	       *(finish_ptr++) = root_ptr->val;
	       root_ptr->val = c_val;
	      }
	    else
	      *(finish_ptr++) = c_val;
	   }
	 *(finish_ptr++) = MAX_VAL;
	}
      if (t < (times -1))
	{temp_ptr = start_ptr;
	 start_ptr = dest_ptr;
	 dest_ptr = temp_ptr;
	}

     }
   finish_ptr = dest_ptr; 
   v = col_size;
   for (i=0;i<adjustments;i++)  
     *(finish_ptr + (v--)) = MAX_VAL;
   
   if (thread_id == (THREADS - 1))
     {col_size = COL_SIZE2;
      *(finish_ptr + col_size + 1) = MAX_VAL;
     }

   free(Tree);
   free(TempT);
   free(Source);
  }
else
  {start_ptr = Buffer1 + thread_id*offset1;
   dest_ptr = Buffer2 + thread_id*offset1;
  }

#if LOCATION 
  {getsysinfo(GSI_CURRENT_CPU, &cpu, sizeof(long), 0, NULL);

   printf ("1:thread = %d, cpu = %d\n",thread_id,cpu);
  }
#endif

#endif
#if 0
 on_one{
 	printf("Buffer2:\n");
 	start_ptr = Buffer2;
	for(i=0; i<2*(((int) (size/M)) + THREADS*THREADS)+size;i++)
	{
		if(start_ptr[i]==NULL) printf(" NULL \n");
		else if(start_ptr[i]<10000) printf( " int(%d) \n",start_ptr[i]);
		 else printf(" %d \n", start_ptr[i]->v1);
	}
	printf("\n");
 }
 node_Barrier();
#endif

if (THREADS > 1)

{

/* SELECT THE SAMPLES */

stride = col_size/samples;   

temp_ptr = (finish_ptr = Sample1 + thread_id*samples) + samples;

s_ptr = dest_ptr;
s_ptr++; //skip off the INT
while (finish_ptr < temp_ptr)
  *(finish_ptr++) =  *(s_ptr += stride);

}
T_end = gethrtime();

interval = T_end-T_start;
/* MERGE THE SAMPLES: */

barrier_wait(t_barrier); 
getclock(TIMEOFDAY,its2_ptr);

T_start = gethrtime();   

if (THREADS > 1)
{

	int * Current,ind;
	TYPE min;
#if 0
if (thread_id == 0)
  {buffer1_ptr = Sample1;  
   buffer2_ptr = Sample2;  

   times = (int) floor((log((double) THREADS)/log((double) 2)) + 0.1); 
   tot = samples/2;

   for (i=1;i<=times;i++)
     {tot <<= 1; 
      finish_ptr = buffer2_ptr;
      t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = buffer1_ptr) + tot) + tot;  
      for (j=0;j<(THREADS >> i);j++)
	{done = 0;
	 while (! done) 
	   {if (myle((*s1_ptr) ,(*s2_ptr)))
	      {*(finish_ptr++) = *(s1_ptr++);
	       if (s1_ptr == t1_ptr) 
		 {while (s2_ptr < t2_ptr)
		    *(finish_ptr++) = *(s2_ptr++);
		  done = 1;
		 }
	      }
	    else
	      {*(finish_ptr++) = *(s2_ptr++);
	       if (s2_ptr == t2_ptr) 
		 {while (s1_ptr < t1_ptr)
		    *(finish_ptr++) = *(s1_ptr++);
		  done = 1;
		 }
	      }
	   }
	 t2_ptr = (s2_ptr = t1_ptr = (s1_ptr = t2_ptr) + tot) + tot;    
	} 
      temp_ptr = buffer1_ptr;
      buffer1_ptr = buffer2_ptr;
      buffer2_ptr = temp_ptr;
     }

   sample_ptr = buffer1_ptr;
  }
#endif

on_one{

#if 0
	printf("sample1:\n");
	for(i=0;i<samples*THREADS;i++)
		printf(" %d ",Sample1[i]);
	printf("samples:%d\n",samples);
#endif
	
	Current = malloc(sizeof(int)*THREADS);
	for(i=0;i<THREADS;i++) Current[i]=0;
	done = THREADS;
	j=0;
	buffer1_ptr=Sample1;
	buffer2_ptr=Sample2;
	while(done !=  0) 
	{
		min = MAX_VAL;
		ind = -1;
		for(i=0;i<THREADS;i++)
		{
			if(Current[i]>=samples) continue;
			if(myle(buffer1_ptr[i*samples+Current[i]],min)) {
				min = buffer1_ptr[i*samples+Current[i]];
				ind = i;
			}
		}
		if (ind != -1) {
			buffer2_ptr[j]=buffer1_ptr[ind*samples+Current[ind]];
			j++;
			Current[ind]++;
			if(Current[ind]>=samples) done--;
		}
	
	}
	temp_ptr = buffer1_ptr;
    buffer1_ptr = buffer2_ptr;
    buffer2_ptr = temp_ptr;
	sample_ptr=buffer1_ptr;
		
	free(Current);
}


#if 0	
on_one {	
	printf("sample2:\n");
	for(i=0;i<samples*THREADS;i++)
		printf(" %d ",Sample2[i]);
	printf("\n");	
	printf("sample_ptr:\n");
	for(i=0;i<samples*THREADS;i++)
		printf(" %d ", sample_ptr[i]);
	printf("\n");
}
#endif

T_end = gethrtime();
interval = T_end-T_start;
node_Barrier();



/* SELECT THE SPLITTERS: */

if (thread_id == 0)
  {stride = col_size/samples;
   sample_ptr -= 1;
   split_ptr = Splitters - 1;
   
   for (i=1;i<THREADS;i++)
     {t_val = (++split_ptr)->val =  *(sample_ptr += samples);
      if (mygt(t_val , (*(sample_ptr - 1))))
	split_ptr->count = stride;
      else
	{l = 0; 
	 r = (i * samples) - 2;
	 t = (l + r) >> 1;
	 done = 0;
	 while (! done)
	   {if (mygt(t_val , (*(buffer1_ptr + t))))
	      {l = t + 1;
	       t = (l + r) >> 1;
	      }
	    else
	      {if (t)
		 {if (mygt(t_val ,(*(buffer1_ptr + t - 1))))
		    {split_ptr->count = ((i * samples) - t) * stride;
		     done = 1;
		    }
		  else
		    {r = t - 1;
		     t = (l + r) >> 1;
		    }
		 }
	       else
		 {split_ptr->count = i * samples * stride;
		  done = 1;
		 }
	      }
	   }
	}
     }
	 
(Splitters + SLAVES)->val = MAX_VAL;
  }



/* DETERMINE THE DESTINATION OF THE INTERMEDIATE ARRAY CONTENTS: */

barrier_wait(t_barrier); 

#if LOCATION 
  {getsysinfo(GSI_CURRENT_CPU, &cpu, sizeof(long), 0, NULL);

   printf ("2:thread = %d, cpu = %d\n",thread_id,cpu);
  }
#endif

start_ptr = start_ptr - thread_id*offset1;
dest_ptr = dest_ptr - thread_id*offset1;

if (thread_id < SLAVES)
  {split_val = (Splitters + thread_id)->val;
   split_count = (Splitters + thread_id)->count;  
#if 0   
   counts_ptr = Counts + thread_id - THREADS;
   buffer1_ptr = dest_ptr - offset1 + 1;
   for (j=0;j<THREADS;j++)
     {done = l = 0;
      if (j == (THREADS - 1))
	values_m1 = COL_SIZE2 - 1;
      else
	values_m1 = COL_SIZE1 -1;
      t = (r = values_m1) >> 1; 
      counts_ptr += THREADS;
      buffer1_ptr += offset1;
      while (! done)
	{if (myle(split_val , (*(buffer1_ptr + t))))
	   {if (! t)
	      done = 1;
	    else
	      {r = t - 1;
	       t = (l + r) >> 1;
	      }
	   }
	 else
	   {if ((t == (values_m1)) || (myle(split_val , (*(buffer1_ptr + t + 1))))) 
	      done = 1;
	    else
	      {l = t + 1;
	       t = (l + r) >> 1;
	      }
	   }
	}
      if ((t == values_m1) || ((t) && mylt(split_val , (*(buffer1_ptr + t + 1)))) ||
	  ((!t) && mygt(split_val , (*buffer1_ptr)) && 
				      mylt(split_val , (*(buffer1_ptr + 1)))))  
	*(counts_ptr) = t + 1;
      else
	{if ((!t) && mylt(split_val , (*buffer1_ptr)))  
	   *(counts_ptr) = 0;
	 else
	   {if ((t == 0)  && (*buffer1_ptr==split_val)) /* myeq */
	      {l = 0; 
	       begin = -1;
	      }
	    else
	      l = (begin = t) + 1;
	    if (((r = t + 4) <= values_m1) && mygt((*(buffer1_ptr + r)),split_val));
	    else   
	      r = values_m1;
	    done = 0;
	    t = (l + r) >> 1;
	    while (! done)
	      {if (mygt(*(buffer1_ptr + t) , split_val))
		 {r = t - 1;
		  t = (l + r) >> 1;
		 } 
	       else  
		 {if ((t == values_m1) || mygt((*(buffer1_ptr + t + 1)) , split_val))
		    done = 1;
		  else
		    {l = t + 1;
		     t = (l + r) >> 1;
		    }
		 }
	      }
	    if (split_count >= (t - begin))
	      {*counts_ptr = t + 1;
	       split_count -= (t - begin);
	      } 
	    else  
	      {*counts_ptr = begin + 1 + split_count;
	       split_count = 0;
	      } 
	   }
	}

     }
#endif
	 
  }
 
  
  node_Barrier();
  
#if 0 
  on_one {
    printf("offset1 is %d, col_size is %d\n", offset1,col_size);
  	for(i=0;i<THREADS;i++)
		printf(" Splitter %d: val = %d, count = %d\n", i,Splitters[i].val->v1, Splitters[i].count);
  } 
  on_one{
  printf(" size is %d\n",2*(((int) (size/M)) + THREADS*THREADS)+size);
  printf("\n-----------\n"); 
  for(i=0;i<2*(((int) (size/M)) + THREADS*THREADS)+size;i++)
  	printf(" %d ", Buffer1[i]);	
  printf("\n");
 
  printf("\n-----------\n");
  for(i=0;i<2*(((int) (size/M)) + THREADS*THREADS)+size;i++)
  	printf(" %d ", Buffer2[i]);
  printf("\n");
  	
 }
 #endif
 
 #if 0
  node_Barrier();	
  sleep(thread_id); 
  printf("thread %d: *start_ptr is %d\n",thread_id, *start_ptr);
  for(i=0;i<4;i++)
  	printf(" %d ", start_ptr[i]);
  printf("\n");
  node_Barrier();
  sleep(thread_id); 
  printf("thread %d: *dest_ptr is %d\n",thread_id, *dest_ptr);
  for(i=0;i<4;i++)
  	printf(" %d ", dest_ptr[i]);
  printf("\n"); 
  printf("thread %d: *dest_ptr is %d\n", thread_id,*dest_ptr);
 #endif
 
barrier_wait(t_barrier); 
getclock(TIMEOFDAY,its3_ptr);   
}


/* Merge the segments : 
	use THREADS-way merge to merge the segments. 
	Memory layout of Buffer2 for 4 threads, 64 input size.
	start_ptr points to Buffer1, and dest_ptr points Buffer2 at this stage
	9  27  28  29  30  31  32  33  34  35  2147483647  83  0  
	9  18  19  20  21  22  23  24  25  26  2147483647  96  97  
	9  9  10  11  12  13  14  15  16  17  2147483647  0  0  
	9  1  2  3  4  5  6  7  8  2147483647  2147483647  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0 
	offset1 is the offset between two sorted segments, in this case 13.
	And the first is always the length of the local sorted list.
*/


#if 1
if(THREADS>1){

	int ** begin_M, ** end_M, * Current, *len_L,*piece_L,*D_start,ind,min,b,x,start_p,id,n,t;
	TYPE * local_start,*T,a,c;
	int * change_P;
	
	temp_ptr = start_ptr;
	start_ptr = dest_ptr;
	dest_ptr = temp_ptr;

#if 0
printf(" start_ptr is %d\n",start_ptr);	
/* check whether the sequential sort part has done its job */
 {
 	//sleep(MYTHREAD);
	printf("sequential sort check start:\n");
	for(i=0; i<col_size-1;i++)
	{
#if 0		
		if(start_ptr[MYTHREAD*offset1+1+i]==NULL) printf(" NULL ");
		else if(start_ptr[MYTHREAD*offset1+1+i]<10000) printf( " int(%d) ",
		start_ptr[MYTHREAD*offset1+1+i]);
		else printf(" (%d,%d,%d) ",	
D[start_ptr[MYTHREAD*offset1+1+i]->v1],D[start_ptr[MYTHREAD*offset1+1+i]->v2],start_ptr[MYTHREAD*offset1+1+i]->w);
#endif

#if 1		
		if(mygt(start_ptr[MYTHREAD*offset1+1+i],start_ptr[MYTHREAD*offset1+1+i+1]))
		{
			printf("THREAD %d: error, %d, %d, %d\n", MYTHREAD,i,start_ptr[MYTHREAD*offset1+1+i]->v1, 
			start_ptr[MYTHREAD*offset1+1+i+1]->v2);
		}
#endif
		
	}
	printf("\n");
	printf("check done \n");
 }
 node_Barrier();
#endif
 
	node_Barrier();			
	local_start = start_ptr + MYTHREAD*offset1+1;
	begin_M = node_malloc(sizeof(int *)*THREADS,TH);
	end_M = node_malloc(sizeof(int *)*THREADS, TH);
	len_L = node_malloc(sizeof(int)*THREADS,TH);
	D_start = node_malloc(sizeof(int)*THREADS,TH);
	
	begin_M[MYTHREAD]=malloc(sizeof(int)*THREADS);
	end_M[MYTHREAD]=malloc(sizeof(int)*THREADS);
	
	for (i=0;i<THREADS-1;i++)
	{
#if 0	
		for(j=0;j<=col_size-1;j++)
			printf(" %d ", local_start[j]);
		printf("\n");	
#endif		
		b=my_bsearch(Splitters[i].val,local_start, 0, col_size-1);
#if 0
		printf("Thread %d: b is %d\n",MYTHREAD,b);
#endif

		begin_M[MYTHREAD][i+1]=b+1;		
		end_M[MYTHREAD][i]=b;
	}
	begin_M[MYTHREAD][0]=0;
	end_M[MYTHREAD][THREADS-1]=col_size-1;
	node_Barrier();

#if 0
	on_one {
		printf("Begin_M:\n");
		for(i=0;i<THREADS;i++)
		{
			for(j=0;j<THREADS;j++)
				printf(" %d\t", begin_M[i][j]);
			printf("\n");
		}
		printf("End_M:\n");
		for(i=0;i<THREADS;i++)
		{
			for(j=0;j<THREADS;j++)
				printf(" %d\t", end_M[i][j]);
			printf("\n");
		}
	}
#endif
		
	piece_L = malloc(sizeof(int)*THREADS);
	len_L[MYTHREAD]=0;
	for(i=0;i<THREADS;i++){
		piece_L[i]=end_M[i][MYTHREAD]-begin_M[i][MYTHREAD]+1;
		len_L[MYTHREAD]+=piece_L[i];
	}
	node_Barrier();
	
	Current = malloc(sizeof(int)*THREADS);
	for(i=0;i<THREADS;i++) Current[i]=1;
	
	
	D_start[MYTHREAD] = len_L[MYTHREAD];
	prefix_sum(D_start,THREADS,TH);
	D_start[MYTHREAD] -= len_L[MYTHREAD];

#if 0	
	on_one {
		printf("D_start:\n");
		for(i=0;i<THREADS;i++)
			printf(" %d ", D_start[i]);
		printf("\n");
	}
	node_Barrier();
	sleep(MYTHREAD);
	printf("piece_L is :\n");
	for(i=0;i<THREADS;i++)
		printf(" %d ", piece_L[i]);
	printf("\n");
#endif
	node_Barrier();
	start_p = D_start[MYTHREAD];
	
	done = THREADS;
	for(i=0;i<THREADS;i++) 
		if( piece_L[i]==0) done--;
	j=0;
	T_start=gethrtime();

#if 0	
	while(done !=  0) 
	{
		//printf("THREAD %d: done is %d\n", MYTHREAD, done);
		min = MAXINT;
		ind = -1;
		for(i=0;i<THREADS;i++)
		{
			if(Current[i]>=piece_L[i]) continue;
			x = i*offset1+1+begin_M[i][MYTHREAD]+Current[i];
			if(start_ptr[x]<=min) {
				min = start_ptr[x];
				ind = i;
			}
		}
		if (ind != -1) {
			dest_ptr[start_p+j]=start_ptr[ind*offset1+1+begin_M[ind][MYTHREAD]+Current[ind]];
			//printf("T%d: %d \n", MYTHREAD,dest_ptr[D_start[MYTHREAD]+j]);
			j++;
			Current[ind]++;
			if(Current[ind]>=piece_L[ind]) done--;
		}
	
	}
#endif
	
	/* Use a tree of losers to do the THREADS-way merge */

#define PARENT(i)       ((i+1)/2-1)
#define LEFT(i)         (2*(i+1)-1)
#define RIGHT(i)        (2*(i+1))
		
	T = malloc(sizeof(TYPE)*(2*THREADS-1));
	change_P = malloc(sizeof(int)*THREADS);
	for(i=0;i<THREADS;i++) {
		if(piece_L[i]==0) T[THREADS-1+i]=MAX_VAL;
		else T[THREADS-1+i]=start_ptr[i*offset1+1+begin_M[i][MYTHREAD]];
	}
	for(i=THREADS-1-1;i>=0;i--)
	{
		l = LEFT(i);
		r = RIGHT(i);
		if( l > 2*THREADS-1-1) a = MAX_VAL;
		else a = T[l];
		if(r > 2*THREADS-1-1) c = MAX_VAL;
		else c = T[r];
		T[i] = MYMIN(a,c);
	}
	j=0;
	
	while(j<len_L[MYTHREAD])
	{
		dest_ptr[start_p+j] = T[0];
		t = 0;
		n=0;
		while(t < 2*THREADS-1){
			change_P[n++]=t;
			if(LEFT(t)<2*THREADS-1 && T[t]==T[LEFT(t)]) t = LEFT(t);
			else t = RIGHT(t);
		}
		i = change_P[n-1];
		id = i-(THREADS-1);
		if(Current[id]>=piece_L[id]) T[i]= MAX_VAL;
		else T[i] = start_ptr[id*offset1+1+begin_M[id][MYTHREAD]+Current[id]];
		for(i=n-2;i>=0;i--)
		{
			x = change_P[i];
			l = LEFT(x);
			r = RIGHT(x);
			if( l > 2*THREADS-1-1) a = MAX_VAL;
			else a = T[l];
			if(r > 2*THREADS-1-1) c = MAX_VAL;
			else c = T[r];
			T[x] = MYMIN(a,c);
		}
		Current[id]++;
		j++;
	}
	free(T);
	free(change_P);
	
	node_Barrier();
	free(begin_M[MYTHREAD]);
	free(end_M[MYTHREAD]);
	free(Current);
	free(piece_L);
	node_Barrier();
	node_free(D_start,TH);
	node_free(len_L,TH);
	node_free(begin_M,TH);
	node_free(end_M,TH);
	
}
T_end = gethrtime();
interval = T_end-T_start;
on_one printf(" merge part sort time is %f s\n", interval/1000000000);

#endif

on_one {
	if (THREADS == 1)
  		dest_ptr++;
	*out_array_ptr = dest_ptr;
	getclock(TIMEOFDAY,its4_ptr);

#if LOCATION 
	{getsysinfo(GSI_CURRENT_CPU, &cpu, sizeof(long), 0, NULL);
   		printf ("4:thread = %d, cpu = %d\n",thread_id,cpu);
	}
#endif

	free(Tree);
	free(TempT);
	free(Source);
	free(Sample1);
	free(Sample2);
	free(Splitters);
	free(Counts);
	free(Number);
	free(Aux);

	}
}

int my_bsearch(TYPE val, TYPE * A, int start, int end)
{

	int l, b, r;
	
	l = start;
	r = end;
	
	b = (l+r)/2;
	
	if(mygt(A[l],val)) return l-1;
	if(mylt(A[r],val)) return r;
	
	while ( r - l > 1)
	{
		if(myeq(A[b],val)) break;
		if(mygt(A[b],val)) r=b;
		else l=b;
		
		b = (l+r)/2;
	} 
	
	if(myeq(A[l],val)) return (l);
	if(myeq(A[r],val)) return (r);
	return (b);
		
}
