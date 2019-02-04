package de.hhu.bsinfo.dxgraph.jobs;

import de.hhu.bsinfo.dxgraph.data.DSString;
import de.hhu.bsinfo.dxgraph.data.FileOffsetDS;
import de.hhu.bsinfo.dxgraph.loader.NullLoader;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;

public class DataLoadingJob extends de.hhu.bsinfo.dxram.job.AbstractJob {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DataLoadingJob.class.getSimpleName());
    private ChunkService m_cs;
    protected static final short MS_TYPE_ID = 100;
    public DataLoadingJob(final long... p_parameterChunkIDs) {
        super(p_parameterChunkIDs);
    }
    @Override
    public short getTypeID() {
        return MS_TYPE_ID;
    }

    @Override
    protected void execute(short p_nodeID, long[] p_chunkIDs) {
        FileOffsetDS ds = new FileOffsetDS(p_chunkIDs[0]);
        m_cs = getService(ChunkService.class);
        m_cs.get().get(ds);

        DSString ds_class = new DSString(p_chunkIDs[1]);
        m_cs.get().get(ds_class);
        String m_LoaderClass = ds_class.getPayload();

        LOGGER.info("Loading %d files from %s",ds.getFileCount(),ds.m_path);

        File fhFolder = new File(ds.m_path);
        if (!fhFolder.exists()) {
            LOGGER.error("path does not exist: %s", ds.m_path);
            return;
        }

        if (!fhFolder.isDirectory()) {
            LOGGER.error("path is not a directory: %s", ds.m_path);
            return;
        }

        File[] files = fhFolder.listFiles();
        NullLoader loaderObject;
        try {
            loaderObject = (NullLoader)Class.forName(m_LoaderClass).getConstructor(ChunkService.class).newInstance(m_cs);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Unknown class "+m_LoaderClass);
            return;
        } catch (Exception e){
            LOGGER.error("Unknown error during loading");
            return;
        }

        File[] filenames = Arrays.copyOfRange(files,ds.m_startFile,ds.m_endFile);
        for(File f : filenames){
            if(loaderObject.isSupported(f.getName())){
                try {
                    LOGGER.info("Loading %s", f.getName());
                    loaderObject.load(f.getAbsolutePath());
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("failed");
                }
            }
        }
        String s = loaderObject.getStats();
        if(s.length() > 0) {
            LOGGER.info("Stats of Loader: " + s);
        }
    }
}
