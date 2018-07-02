package de.hhu.bsinfo.dxgraph.loader;

import de.hhu.bsinfo.dxgraph.loader.AbstractLoader;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

public class NullLoader{
    protected DXRAMServiceAccessor m_dxramServiceAccessor;
    protected ChunkService m_cs;
    public NullLoader(DXRAMServiceAccessor dxramServiceAccessor){
        m_dxramServiceAccessor = dxramServiceAccessor;
        m_cs = m_dxramServiceAccessor.getService(ChunkService.class);
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
