package de.hhu.bsinfo.dxgraph.tasks;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxgraph.data.FileOffsetDS;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by philipp on 27.06.17.
 */
public class LoadingSchedulerTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LoadingSchedulerTask.class.getSimpleName());

    public static final String MS_ROOTS = "LS";

    @Expose
    private String m_path = "./";

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_path.length());
        p_exporter.writeBytes(m_path.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void importObject(final Importer p_importer) {
        int strLength =0;
        strLength= p_importer.readInt(strLength);
        byte[] tmp = new byte[strLength];
        p_importer.readBytes(tmp);
        m_path = new String(tmp, StandardCharsets.UTF_8);
    }

    @Override
    public int execute(TaskContext p_ctx) {
        if (p_ctx.getCtxData().getSlaveId() == 0) {
            TemporaryStorageService temporaryStorageService = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
            NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);


            ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);


            short[] slaves = p_ctx.getCtxData().getSlaveNodeIds();
            LOGGER.info("Found %d peers", slaves.length);
            LOGGER.info("m_path %s", m_path);
            File fhFolder = new File(m_path);
            if (!fhFolder.exists()) {
                // #if LOGGER >= ERROR
                LOGGER.error("path does not exist: %s", m_path);
                // #endif /* LOGGER >= ERROR */
                return -1;
            }

            if (!fhFolder.isDirectory()) {
                // #if LOGGER >= ERROR
                LOGGER.error("path is not a directory: %s", m_path);
                // #endif /* LOGGER >= ERROR */
                return -2;
            }
            File[] files = fhFolder.listFiles();
            LOGGER.info("Found %d files", files.length);
            double filesPerPeer = Math.ceil(files.length/slaves.length);
            for(int i=0;i<slaves.length;i++){
                int startFile = (int)(i*filesPerPeer);
                int endFile = (int)Math.min((i+1)*filesPerPeer,files.length);
                int fileCount = endFile-startFile;
                FileOffsetDS ds = new FileOffsetDS(startFile,endFile,m_path);
                LOGGER.info("Node 0x%X files: %d", slaves[i], fileCount);
                chunkService.create(ds);
                chunkService.put(ds);
                nameserviceService.register(ds, ""+i);
            }
            DSByteBuffer ds = new DSByteBuffer(ByteBuffer.allocate(4).putInt(files.length));
            chunkService.create(ds);
            chunkService.put(ds);
        }
        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
        switch (p_signal) {
            case SIGNAL_ABORT: {
                // ignore signal here
                break;
            }
        }
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + m_path.length();
    }
}
