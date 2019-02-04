package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Created by philipp on 13.07.17.
 */
public class FileOffsetDS extends AbstractChunk {
    public int m_startFile;
    public int m_endFile;
    public String m_path;
    public FileOffsetDS(){
        super();
    }
    public FileOffsetDS(long p_chunkID){
        super(p_chunkID);
    }
    public FileOffsetDS(int p_start, int p_end, String p_path){
        m_startFile = p_start;
        m_endFile = p_end;
        m_path = p_path;
    }
    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_startFile);
        p_exporter.writeInt(m_endFile);
        p_exporter.writeString(m_path);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_startFile = p_importer.readInt(m_startFile);
        m_endFile  = p_importer.readInt(m_endFile);
        m_path = p_importer.readString(m_path);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES*2+ ObjectSizeUtil.sizeofString(m_path);
    }

    public int getFileCount(){
        return m_endFile-m_startFile;
    }
}
