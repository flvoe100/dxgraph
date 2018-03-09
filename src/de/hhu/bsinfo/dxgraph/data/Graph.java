package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.ArrayList;

public class Graph extends DataStructure {
    private ArrayList<Edge> edges;
    private ArrayList<Vertex> vertices;
    public Graph(){
        edges=new ArrayList<Edge>();
        vertices=new ArrayList<Vertex>();
    }
    public ArrayList<Vertex> getVertices(){
        return vertices;
    }
    public ArrayList<Edge> getEdges(){
        return edges;
    }
    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(vertices.size());
        for (Vertex e : vertices){
            p_exporter.exportObject(e);
        }
        p_exporter.writeInt(edges.size());
        for (Edge e : edges){
            p_exporter.exportObject(e);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        int num_vertices = 0;
        int num_edges = 0;
        num_vertices = p_importer.readInt(num_vertices);
        for(int i=0; i<num_vertices;i++){
            Vertex nv = new Vertex();
            p_importer.importObject(nv);
            vertices.add(nv);
        }
        num_edges = p_importer.readInt(num_edges);
        for(int i=0; i<num_edges;i++){
            Edge ne = new Edge();
            p_importer.importObject(ne);
            edges.add(ne);
        }
    }

    @Override
    public int sizeofObject() {
        int s = 0;
        s += Integer.BYTES;
        for (Vertex e : vertices){
            s += e.sizeofObject();
        }
        s += Integer.BYTES;
        for (Edge e : edges){
            s += e.sizeofObject();
        }

        return s;
    }
}
