#!/bin/bash
#gcc -g -O2 -shared -fpic -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -I"../deps/nauty/" -L"../deps/nauty/" ../deps/nauty/naugraph.c ../deps/nauty/nauty.c ../deps/nauty/showg.c ../deps/nauty/nautil.c JNINauty.c -E
gcc -g -O2 -shared -fpic -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -I"../deps/nauty/" -L"../deps/nauty/" ../deps/nauty/naugraph.c ../deps/nauty/nauty.c ../deps/nauty/showg.c ../deps/nauty/nautil.c JNINauty.c -Wl,-soname,libJNINauty.so -o libJNINauty.so
#gcc -O0 -shared -fpic -I"$JAVA_HOME/include" -I"$JAVA_HOME/include/linux" -I"../nauty/" -L"../nauty/" -o libNautyCheck.so de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty.c
#gcc -Wl,--add-stdcall-alias -shared -o libNautyCheck.so de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty.o
#gcc -shared -o libNautyCheck.so de_hhu_bsinfo_dxgraph_algo_isomorphy_JNINauty.o
echo $?
