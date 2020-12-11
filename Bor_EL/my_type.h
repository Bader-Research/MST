#ifndef _MY_TYPE_H_
#define _MY_TYPE_H_

typedef struct q_ele{
int key,v,u;
}q_ele_t;

#define KEY q_ele_t
#define ID(A) A.v

#endif

