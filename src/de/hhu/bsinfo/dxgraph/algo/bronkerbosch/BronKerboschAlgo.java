package de.hhu.bsinfo.dxgraph.algo.bronkerbosch;

import de.hhu.bsinfo.dxgraph.data.Graph;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.util.ArrayList;

public class BronKerboschAlgo extends Thread {
    private ArrayList<Vertex> input = null;
    private ChunkService cs;
    private ArrayList<String> result;
    public BronKerboschAlgo(){
        cs = new ChunkService();
        result = new ArrayList<String>();
    }
    public BronKerboschAlgo(Graph g){
        super();
        input = g.getVertices();
    }
    public BronKerboschAlgo(ArrayList<Vertex> i){
        super();
       input = i;
    }

    public ArrayList<String> getResult() {
        return result;
    }

    private void BronKerboschStep(ArrayList<Vertex> r, ArrayList<Vertex> p, ArrayList<Vertex> x){
        if (p.isEmpty() && x.isEmpty()){
            result.add(r.toString());
            return;
        }
        for (Vertex v : p){
            ArrayList<Vertex> rn = (ArrayList<Vertex>)r.clone();
            rn.add(v);
            ArrayList<Vertex> neighbours = getNeighbours(v);
            ArrayList<Vertex> pn = (ArrayList<Vertex>)p.clone();
            pn.removeAll(neighbours);
            ArrayList<Vertex> xn = (ArrayList<Vertex>) x.clone();
            xn.removeAll(neighbours);
            BronKerboschStep(rn, pn, xn);
            p.remove(v);
            x.add(v);
        }
    }

    private ArrayList<Vertex> getNeighbours(Vertex v){
        ArrayList<Vertex> al = new ArrayList<Vertex>();
        for(long l : v.getNeighbours()){
            Vertex nv = new Vertex();
            nv.setID(l);
            cs.get(nv);
            al.add(nv);
        }
        return al;
    }

    public void run() {
        BronKerboschStep(new ArrayList<Vertex>(), input, new ArrayList<Vertex>());
    }

}
