#include <jni.h>
#include <stdio.h>
#include "JNINauty.h"
#include <nauty.h>
static int n;
static int m;
DYNALLSTAT(int, ptn, ptn_sz);
DYNALLSTAT(int, orbits, orbits_sz);
DYNALLSTAT(int, map, map_sz);

// Graph 1
DYNALLSTAT(int, lab1,lab1_sz);
DYNALLSTAT(graph, g1, g1_sz);

// Graph 2
DYNALLSTAT(int, lab2,lab2_sz);
DYNALLSTAT(graph, g2, g2_sz);
/*
 * Class:     de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty
 * Method:    init
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty_init
  (JNIEnv *env, jobject this, jint nn){
    n = nn;

  	m = SETWORDSNEEDED(n);
  	nauty_check(WORDSIZE,m,n,NAUTYVERSIONID);

  	DYNALLOC1(int, ptn, ptn_sz, n, "malloc" );
  	DYNALLOC1(int, orbits, orbits_sz, n, "malloc" );
  	DYNALLOC1(int, map,map_sz,n,"malloc");

  	DYNALLOC1(int, lab1, lab1_sz,n,"malloc");
  	DYNALLOC2(graph, g1, g1_sz,n,m,"malloc");
  	EMPTYGRAPH(g1,m,n);


  	DYNALLOC1(int, lab2, lab2_sz,n,"malloc");
  	DYNALLOC2(graph, g2, g2_sz,n,m,"malloc");
  	EMPTYGRAPH(g2,m,n);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty_addEdge
  (JNIEnv *env, jobject this, jint g , jint v, jint w){
  	if (g == 0){
  		ADDONEEDGE(g1,v,w,m);
  	} else {
  		ADDONEEDGE(g2,v,w,m);
  	}
  }
JNIEXPORT jboolean JNICALL Java_de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty_checkIsomorpy
  (JNIEnv *env, jobject this){
  DYNALLSTAT(graph, cg1, cg1_sz);
  DYNALLSTAT(graph, cg2, cg2_sz);
    DEFAULTOPTIONS_GRAPH(options);
    options.getcanon = TRUE;
  	DYNALLOC2(graph, cg1, cg1_sz, n, m, "malloc")
  	DYNALLOC2(graph, cg2, cg2_sz, n, m, "malloc")
  	 statsblk stats;
  	densenauty(g1,lab1,ptn,orbits, &options, &stats, m,n, cg1);
  	densenauty(g2,lab2,ptn,orbits, &options, &stats, m,n, cg2);
    size_t k;
  	for( k = 0; k< m*(size_t)n; ++k){
  		if(cg1[k] != cg2[k]){
  			return JNI_FALSE;
  		}
  	}
  		return JNI_TRUE;
  }
