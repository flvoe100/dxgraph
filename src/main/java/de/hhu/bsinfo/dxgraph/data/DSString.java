package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class DSString extends DataStructure {
    private String m_string;
    public DSString(String p_string){
        m_string = p_string;
    }
    public DSString(long id){
        super(id);
    }
    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_string);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_string = p_importer.readString(m_string);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_string);
    }
    public String getPayload(){
        return m_string;
    }
}
