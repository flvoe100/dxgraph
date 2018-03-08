package de.hhu.bsinfo.dxgraph.job;

import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.job.JobNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CalculationJob extends AbstractJob
{
    private static final Logger LOGGER = LogManager.getFormatterLogger(CalculationJob.class.getSimpleName());

    public static final short MS_TYPE_ID = 1337;

    public CalculationJob(final long... p_parameterChunkIDs){
        super(p_parameterChunkIDs);
    }

    @Override
    public short getTypeID() {
        return 0;
    }

    @Override
    protected void execute(short p_nodeID, long[] p_chunkIDs) {

    }
}
