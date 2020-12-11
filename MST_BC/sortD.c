#include "../grph.h"
#include "simple.h"
#include "merge.h"

#define bits(x,k,j) ((x>>k) & ~(~0<<j))

typedef ent_t DATA;


/****************************************************/
void all_countsort_smp(int q,
			DATA *inArr,
			DATA *lSorted,
			int R,
			int bitOff, int m,
			THREADED)
/****************************************************/
/* R (range)      must be a multiple of SMPS */
/* q (elems/proc) must be a multiple of SMPS */
{
    register int
	j,
	k,
        last, temp,
	offset;
    
    int *myHisto,
        *mhp,
        *mps,
        *psHisto,
        *allHisto;

    myHisto  = (int *)node_malloc(THREADS*R*sizeof(int), TH);
    psHisto  = (int *)node_malloc(THREADS*R*sizeof(int), TH);

    mhp = myHisto + MYTHREAD*R;

    for (k=0 ; k<R ; k++)
      mhp[k] = 0;
    
    pardo(k, 0, q, 1)
      mhp[bits(KEY(inArr[k]),bitOff,m)]++;

    node_Barrier();

    pardo(k, 0, R, 1) {
      last = psHisto[k] = myHisto[k];
      for (j=1 ; j<THREADS ; j++) {
	temp = psHisto[j*R + k] = last + myHisto[j*R +  k];
	last = temp;
      }
    }

    allHisto = psHisto+(THREADS-1)*R;

    node_Barrier();
    
    offset = 0;

    mps = psHisto + (MYTHREAD*R);
    for (k=0 ; k<R ; k++) {
      mhp[k]  = (mps[k] - mhp[k]) + offset;
      offset += allHisto[k];
    }

    node_Barrier();
    
    pardo(k, 0, q, 1) {
      j = bits(KEY(inArr[k]),bitOff,m);
      lSorted[mhp[j]] = inArr[k];
      mhp[j]++;
    }

    node_Barrier();

    node_free(psHisto, TH);
    node_free(myHisto, TH);
}

/****************************************************/
void all_radixsort_check(int q,
			 DATA *lSorted)
/****************************************************/
{
  int i;

  for (i=1; i<q ; i++) 
    if (KEY(lSorted[i-1]) > KEY(lSorted[i])) {
      fprintf(stderr,
	      "ERROR: q:%d lSorted[%6d] > lSorted[%6d] (%6d,%6d)\n",
	      q,i-1,i,KEY(lSorted[i-1]),KEY(lSorted[i]));
    }
}

/****************************************************/
void all_radixsort_smp_s3(int q,
			   DATA *inArr,
			   DATA *lSorted,
			   THREADED)
/****************************************************/
{
  DATA *lTemp;

    lTemp = (DATA *)node_malloc(q*sizeof(DATA), TH);
			
    all_countsort_smp(q, inArr,   lSorted, (1<<11),  0, 11, TH);
    all_countsort_smp(q, lSorted, lTemp,   (1<<11), 11, 11, TH);
    all_countsort_smp(q, lTemp,   lSorted, (1<<10), 22, 10, TH);

    node_free(lTemp, TH);
}

/****************************************************/
void all_radixsort_smp_s2(int q,
			   DATA *inArr,
			   DATA *lSorted,
			   THREADED)
/****************************************************/
{
  DATA *lTemp;

    lTemp = (DATA *)node_malloc(q*sizeof(DATA), TH);
			
    all_countsort_smp(q, inArr,   lTemp,   (1<<16),  0, 16, TH);
    all_countsort_smp(q, lTemp,   lSorted, (1<<4), 16, 4, TH);

    node_free(lTemp, TH);
}

/****************************************************/
void all_radixsort20_smp_s1(int q,
			     DATA *inArr,
			     DATA *lSorted,
			     THREADED)
/****************************************************/
{
    all_countsort_smp(q, inArr,   lSorted, (1<<20),  0, 20, TH);
}

/****************************************************/
void all_radixsort20_smp_s2(int q,
			     DATA *inArr,
			     DATA *lSorted,
			     THREADED)
/****************************************************/
{
  DATA *lTemp;

    printf("Before node malloc\n");
    lTemp = (DATA *)node_malloc(q*sizeof(DATA), TH);
     printf("after node malloc\n");
     		
    all_countsort_smp(q, inArr,   lTemp,   (1<<10),  0, 10, TH);
    all_countsort_smp(q, lTemp,   lSorted, (1<<10), 10, 10, TH);

    node_free(lTemp, TH);
}
