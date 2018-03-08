package de.hhu.bsinfo.dxgraph.algo.isomorphy;

import java.util.Map;

public interface IsomorphyInterface {
    public abstract boolean check(int vertices1, Map<Integer,Integer> edges1, int vertices2, Map<Integer,Integer> edges2);
}
