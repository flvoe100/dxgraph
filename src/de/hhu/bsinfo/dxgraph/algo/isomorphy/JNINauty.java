package de.hhu.bsinfo.dxgraph.algo.isomorphy;

import java.util.HashMap;
import java.util.Map;

public class JNINauty implements IsomorphyInterface {
    public JNINauty(){
        System.loadLibrary("JNINauty");
    }
    private native void init(int vertices);
    private native void addEdge(int graph, int v, int w);
    private native boolean checkIsomorpy();
    public boolean check(int vertices1, Map<Integer,Integer> edges1,int vertices2, Map<Integer,Integer> edges2){
        if(vertices1 != vertices2){
            return false;
        }
        if(edges1.size() != edges2.size()){
            return false;
        }
        this.init(vertices1);
        for (Map.Entry<Integer,Integer> edge : edges1.entrySet()){
            this.addEdge(0,(int)edge.getKey(),(int)edge.getValue());
        }
        for (Map.Entry<Integer,Integer> edge : edges2.entrySet()){
            this.addEdge(1,(int)edge.getKey(),(int)edge.getValue());
        }

        return this.checkIsomorpy();
    }
    public static void main(String[] args){
        HashMap edges_1 = new HashMap();
        edges_1.put(0,1);
        edges_1.put(1,2);
        HashMap edges_2 = new HashMap();
        edges_2.put(0,2);
        edges_2.put(2,1);
        edges_2.put(2,0);
        JNINauty checker = new JNINauty();
        System.out.println(checker.check(4,edges_1,4,edges_1));
        System.out.println(checker.check(3,edges_1,3,edges_2));
    }
}
