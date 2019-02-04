package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.lang.reflect.Array;

public class List<T> extends AbstractChunk {
    private Class<T> m_datatype;
    private T[] m_data;
    private int m_current;
    public List(Class<T> clazz, int capacity){
        m_data = (T[]) Array.newInstance(clazz, capacity);
        m_datatype = clazz;
        m_current = 0;
    }
    @Override
    public void exportObject(Exporter p_exporter) {
        //p_exporter.writeString(m_datatype.getTypeName());
        p_exporter.writeInt(m_data.length);
        p_exporter.writeInt(m_current);
        for(int i=0; i<m_current;i++){
            T e = m_data[i];
            if (m_datatype == Integer.class){
                p_exporter.writeInt((Integer) e);
            } else if (m_datatype == Long.class){
                p_exporter.writeLong((Long) e);
            }
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        /**String className = p_importer.readString();
        try if(m_datatype == String.class){
                this.addElement((T)p_importer.readString());
            } else {
            m_datatype = Class.forName(className);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }*/
        int size = 0;
        size = p_importer.readInt(size);
        int end = 0;
        end = p_importer.readInt(end);
        m_data = (T[]) Array.newInstance(m_datatype, size);
        for (int i=0; i<end;i++){
            if (m_datatype == Integer.class){
                int t = 0;
                this.addElement((T)m_datatype.cast(p_importer.readInt(t)));
            } else if (m_datatype == Long.class){
                long t = 0;
                this.addElement((T)m_datatype.cast(p_importer.readLong(t)));
            }
        }
    }

    private int calcSize(){
        int size = 0;
        if (m_datatype == Integer.class){
            size += Integer.BYTES * m_data.length;
        }else if (m_datatype == Long.class){
            size += Long.BYTES * m_data.length;
        }

        return size;
    }

    public void addElement(T elem){
        m_data[m_current]=elem;
        m_current++;
    }

    public T[] getData() {
        return m_data;
    }

    @Override
    public int sizeofObject() {
        return 2*Integer.BYTES+this.calcSize();
    }
}
