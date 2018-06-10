package de.hhu.bsinfo.dxgraph.algo.bronkerbosch;

import de.hhu.bsinfo.dxgraph.DXGraph;
import de.hhu.bsinfo.dxgraph.data.Graph;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.util.ArrayList;

public class BronKerboschAlgo {
    private DXGraph input = null;
    private ChunkService cs;
    private ArrayList<ArrayList<Vertex>> result;
    public BronKerboschAlgo(){
        cs = new ChunkService();
        result = new ArrayList<ArrayList<Vertex>>();
    }
    public BronKerboschAlgo(DXGraph g){
        super();
        input = g;
    }
    public BronKerboschAlgo(ArrayList<Vertex> i){
        super();
       input = new DXGraph();
       for (Vertex v : i){
           input.createVertices(v);
       }
    }

    public ArrayList<ArrayList<Vertex>> getResult() {
        BronKerboschStep(new ArrayList<Vertex>(), input, new ArrayList<Vertex>());
        return result;
    }

    private void BronKerboschStep(ArrayList<Vertex> r, ArrayList<Vertex> p, ArrayList<Vertex> x){
        if (p.isEmpty() && x.isEmpty()){
            result.add(r);
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
}
