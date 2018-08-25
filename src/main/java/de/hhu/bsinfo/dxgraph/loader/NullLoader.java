package de.hhu.bsinfo.dxgraph.loader;

import de.hhu.bsinfo.dxgraph.loader.AbstractLoader;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

public class NullLoader{
    protected DXRAMServiceAccessor m_dxramServiceAccessor;
    protected ChunkService m_cs;
    public NullLoader(ChunkService p_cs){
        m_cs = p_cs;
    }
    public boolean isSupported(String path) {
        return false;
    }

    public void load(String path) {

    }
    public String getStats(){
        return "";
    }
}
