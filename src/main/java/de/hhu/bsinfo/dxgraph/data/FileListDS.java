package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.util.ArrayList;

/**
 * Created by philipp on 27.06.17.
 */
public class FileListDS extends DataStructure {
    private String[] m_paths;
    public FileListDS(){
        m_paths = null;
    }
    public FileListDS(ArrayList<String> p_files){
        m_paths = p_files.toArray(new String[0]);
    }
    @Override
    public void exportObject(final Exporter p_exporter) {
        //p_exporter.writeStringArray(m_paths);

    }

    public String[] getPaths(){
        //return 0;
        return m_paths;
    }

    @Override
    public void importObject(final Importer p_importer) {
        //m_paths = p_importer.readStringArray();
    }

    @Override
    public int sizeofObject() {
        return 0;
        /*if(m_paths.length>0){
            return Integer.BYTES + m_paths.length*ObjectSizeUtil.sizeofString(m_paths[0]);
        } else {
            return Integer.BYTES;
        }*/
    }
}

