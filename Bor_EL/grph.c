#include <stdlib.h>
#include <stdio.h>
#include "../grph.h"
#include <string.h>

int read_graph(char * graph_name,int * n_vertices, int *d_or_s,ent_t** p_grph_list, int *** p_grph_matrix )
{
#define LINELEN 10000
  FILE * fp;
  int ** grph_matrix;
  ent_t * grph_list;
  char  description[LINELEN];
  char  pTemp[LINELEN];
  int n,i,j,w,*D;
 
  fp = fopen(graph_name,"rt");
  fscanf(fp,"%s",description);

  if(strstr(description,"dense")) {
    *d_or_s=DENSE;
    fscanf(fp,"%d",&n );
    grph_matrix =(int **) malloc (sizeof(int*)*n);
    for(i=0;i<n;i++)
       grph_matrix[i]=(int *)malloc(sizeof(int)*n);
    for(i=0;i<n;i++)
      for(j=0;j<n;j++)
      {
         fscanf(fp,"%d",&w);
	 grph_matrix[i][j]=w;
      }
  }
  else if(strstr(description,"sparse")){
    fscanf(fp,"%d",&n );
    *d_or_s=SPARSE;
    grph_list = (ent_t *)malloc(sizeof(ent_t)*n);
    D =(int*) malloc(sizeof(int)*n);
    fgets(pTemp,LINELEN,fp); //skip the new line symbol
    for(i=0;i<n;i++)
    {
	fgets(pTemp,LINELEN,fp);
        sscanf(pTemp,"%d",&D[i]);
    }
    for(i=0;i<n;i++)
    {
	grph_list[i].n_neighbors=D[i];
        grph_list[i].my_neighbors=(ele_t*)malloc(sizeof(ele_t)*D[i]);
    }
    rewind(fp);
    fgets(pTemp,LINELEN,fp); //skip the description line
    fgets(pTemp,LINELEN,fp); //skip the <n> line
    for(i=0;i<n;i++)
    {
      fscanf(fp,"%d",&w); // skip the degree section
      for(j=0;j<D[i];j++)
        fscanf(fp, "%d %d", &grph_list[i].my_neighbors[j].v, & grph_list[i].my_neighbors[j].w);
     } 
     free(D);
  } else {
    printf("First line should specify the representation of the graph\n");
    fclose(fp);
    return (-1);
  }

  fclose(fp);
  * p_grph_list=grph_list;
  * p_grph_matrix = grph_matrix;
  * n_vertices =n;
  return 0; 
}

int edge_number(ent_t *grph_list, int n_vertices)
{

  int n=0,i;

  for(i=0;i<n_vertices;i++)
	n+=grph_list[i].n_neighbors;

  return(n/2);
}
