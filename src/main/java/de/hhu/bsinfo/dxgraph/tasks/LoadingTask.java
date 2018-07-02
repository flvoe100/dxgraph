package de.hhu.bsinfo.dxgraph.tasks;


import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxgraph.data.FileOffsetDS;
import de.hhu.bsinfo.dxgraph.loader.NullLoader;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;

/**
 * Created by philipp on 08.07.17.
 */
public class LoadingTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoadingTask.class.getSimpleName());

    @Expose
    private String m_LoaderClass = "de.hhu.bsinfo.dxgraph.loader.NullLoader";

    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public void importObject(Importer p_importer) {

    }

    @Override
    public int execute(TaskContext p_ctx) {
        DXRAMServiceAccessor dxServiceAccessor =  p_ctx.getDXRAMServiceAccessor();
        TemporaryStorageService temporaryStorageService = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
        NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        short slaveId = p_ctx.getCtxData().getSlaveId();
        short myNodeId = p_ctx.getCtxData().getOwnNodeId();
        long chunkId = nameserviceService.getChunkID(""+slaveId, 5000);
        if(chunkId == ChunkID.INVALID_ID) {
            LOGGER.error("Could not find my chunk with filenames");
        } else{
            LOGGER.info("Found chunk 0x%X",chunkId);
        }
        FileOffsetDS ds = new FileOffsetDS();
        ds.setID(chunkId);
        chunkService.get(ds);
        LOGGER.info("Loading %d files from %s",ds.getFileCount(),ds.m_path);

        File fhFolder = new File(ds.m_path);
        if (!fhFolder.exists()) {
            // #if LOGGER >= ERROR
            LOGGER.error("path does not exist: %s", ds.m_path);
            // #endif /* LOGGER >= ERROR */
            return -1;
        }

        if (!fhFolder.isDirectory()) {
            // #if LOGGER >= ERROR
            LOGGER.error("path is not a directory: %s", ds.m_path);
            // #endif /* LOGGER >= ERROR */
            return -2;
        }

        File[] files = fhFolder.listFiles();
        NullLoader loaderObject;
        try {
            loaderObject = (NullLoader)Class.forName(m_LoaderClass).getConstructor(DXRAMServiceAccessor.class).newInstance(dxServiceAccessor);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Unknown class "+m_LoaderClass);
            e.printStackTrace();
            return -2;
        } catch (Exception e){
            LOGGER.error("Unknown error during loading");
            e.printStackTrace();
            return -2;
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
                    //return -1;
                }
            }
        }
        String s = loaderObject.getStats();
        if(s.length() > 0) {
            LOGGER.info("Stats of Loader: " + s);
        }
        // }
        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
