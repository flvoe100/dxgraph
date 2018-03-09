#!/bin/bash

java -cp "../app/dxgraph.jar:../../dxram/dxram.jar" -Djava.library.path="../jni/" de.hhu.bsinfo.dxgraph.algo.isomorphy.JNINauty
