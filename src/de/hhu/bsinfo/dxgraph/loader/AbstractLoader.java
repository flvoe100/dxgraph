package de.hhu.bsinfo.dxgraph.loader;


abstract class AbstractLoader {
    abstract public boolean isSupported(String path);
    abstract public void load(String path);
    public String getStats(){
        return "";
    }
}