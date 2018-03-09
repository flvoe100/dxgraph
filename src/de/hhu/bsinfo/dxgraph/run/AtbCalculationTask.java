package de.hhu.bsinfo.dxgraph.run;

import de.hhu.bsinfo.dxatb.data.List;
import de.hhu.bsinfo.dxgraph.job.CalculationJob;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;

public class AtbCalculationTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AtbCalculationTask.class.getSimpleName());
    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public void importObject(Importer p_importer) {

    }
    @Override
    public int execute(TaskContext p_ctx) {
        TemporaryStorageService temporaryStorageService = p_ctx.getDXRAMServiceAccessor().getService(TemporaryStorageService.class);
        NameserviceService nameserviceService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        NetworkService m_networkService = p_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        JobService jobService = p_ctx.getDXRAMServiceAccessor().getService(JobService.class);
        short slaveId = p_ctx.getCtxData().getSlaveId();
        short[] nodes = p_ctx.getCtxData().getSlaveNodeIds();
        short myNodeId = nodes[slaveId];
        short[] workerIds = Arrays.copyOfRange(nodes,1,nodes.length);
        if(slaveId == 0){
            LOGGER.info("I'm the leader");
            long ATBRL_chunkID = nameserviceService.getChunkID("ATBRL",5000);
            DSByteBuffer ds = new DSByteBuffer(ATBRL_chunkID,4);
            chunkService.get(ds);

            long ATBR_chunkID = nameserviceService.getChunkID("ATBR", 5000);
            List<Long> ll = new List<Long>(Long.class,ds.getData().getInt());
            ll.setID(ATBR_chunkID);
            chunkService.get(ll);
            LOGGER.info("Loaded ATBR chunk 0x%X",ll.getID());
            Long[] allGraphs = ll.getData();
            for(long chunkId1 : allGraphs){
                LOGGER.info("Sending Workpackets for  0x%X",chunkId1);
                for(short slave : workerIds){
                    for(long chunkId2 : allGraphs){
                        long[] params = {chunkId1,chunkId2};
                        CalculationJob cj = new CalculationJob(chunkId1,chunkId2);
                        jobService.pushJobRemote(cj,slaveId);
                    }
                }
            }
            jobService.waitForAllJobsToFinish();
        }else{
            LOGGER.info("Waiting for Worktasks");
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
        return 0;
    }
}
