package de.hhu.bsinfo.dxgraph.loader;

import de.hhu.bsinfo.dxgraph.data.Edge;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EdgeFileLoader extends NullLoader {
    private int m_edegs,m_vertices;
    private ChunkService m_cs;
    public EdgeFileLoader(ChunkService p_cs) {
        super(p_cs);
        m_cs = p_cs;
        m_edegs = 0;
        m_vertices = 0;
    }
    public boolean isSupported(String path) {
        return path.endsWith(".ef");
    }

    public void load(String path) {
        try {
            List<String> m_lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
            Map<String, Vertex> vertex_id_map = new HashMap<String, Vertex>();
            for(String l : m_lines){
                String[] values = l.split("\t");
                String v1 = values[0];
                String v2 = values[1];
                Vertex v1_vertex;
                if(vertex_id_map.containsKey(v1)){
                    v1_vertex = vertex_id_map.get(v1);
                } else {
                    v1_vertex = new Vertex();
                    v1_vertex.setNeighborsAreEdgeObjects(true);
                    vertex_id_map.put(v1,v1_vertex);
                   // m_cs.reserve(v1_vertex);
                    m_cs.create(v1_vertex);
                    m_cs.put(v1_vertex);
                    m_vertices++;
                }
                Vertex v2_vertex;
                if(vertex_id_map.containsKey(v2)){
                    v2_vertex = vertex_id_map.get(v2);
                } else {
                    v2_vertex = new Vertex();
                    v2_vertex.setNeighborsAreEdgeObjects(true);
                    vertex_id_map.put(v2, v2_vertex);
                    //m_cs.reserve(v2_vertex);
                    m_cs.create(v2_vertex);
                    m_cs.put(v2_vertex);
                    m_vertices++;
                }
                Edge e = new Edge();
                m_cs.create(e);
                e.setFromId(v1_vertex.getID());
                e.setToId(v2_vertex.getID());
                v1_vertex.addNeighbour(e.getID());
                v2_vertex.addNeighbour(e.getID());
                m_cs.put(e);
                m_edegs++;
            }
            for(Map.Entry<String,Vertex> entry: vertex_id_map.entrySet()){
                Vertex v = entry.getValue();
                m_cs.create(v);
                m_cs.put(v);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public String getStats(){
        return "vertices:"+m_vertices+", edges:"+m_edegs;
    }
}
