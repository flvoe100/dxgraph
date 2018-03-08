package de.hhu.bsinfo.dxgraph.run;


import de.hhu.bsinfo.dxatb.data.FileListDS;
import de.hhu.bsinfo.dxatb.data.FileOffsetDS;
import de.hhu.bsinfo.dxatb.data.List;
import de.hhu.bsinfo.dxatb.utils.LgfParser;
import de.hhu.bsinfo.dxgraph.data.Edge;
import de.hhu.bsinfo.dxgraph.data.LabelProperty;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxgraph.data.VertexSimple;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DSByteBuffer;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.DummyDataStructure;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by philipp on 08.07.17.
 */
public class AtbLoadingTask implements Task {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AtbLoadingTask.class.getSimpleName());
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
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        short slaveId = p_ctx.getCtxData().getSlaveId();
        if(slaveId == 0){
            List<Long> ll = new List<Long>(Long.class,4);
            chunkService.create(ll);
            chunkService.put(ll);
            nameserviceService.register(ll,"ASTR");
        }
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

        long ATBRL_chunkID = nameserviceService.getChunkID("ATBRL",5000);
        DSByteBuffer dsATBRL = new DSByteBuffer(ATBRL_chunkID,4);
        chunkService.get(dsATBRL);
        long ATBR_chunkID = nameserviceService.getChunkID("ATBR", 5000);
        List<Long> ll = new List<Long>(Long.class,dsATBRL.getData().getInt());
        ll.setID(ATBR_chunkID);

            File[] files = fhFolder.listFiles();

            File[] filenames = Arrays.copyOfRange(files,ds.m_startFile,ds.m_endFile);
            for(File f : filenames){
                VertexSimple root;
                if(f.getName().endsWith(".lgf")) {
                    try {
                        LOGGER.info("Loading %s", f.getName());
                        LgfParser lgf_parser = new  LgfParser(f.getAbsolutePath());
                        Map<String, HashMap<String, String>> vertices = lgf_parser.getVerticies();
                        Map<String, String[]> edges = lgf_parser.getEdges();
                        Map<String, Long> vertex_id_map = new HashMap<String, Long>();
                        Map<Long, VertexSimple> vertex_map = new HashMap<>();
                        LOGGER.info("parsed %d vertices and %d edges", vertices.size(), edges.size());
                        root = null;
                        for (Map.Entry<String, HashMap<String, String>> entry : vertices.entrySet()) {
                            //LOGGER.info("creating Vertix ");
                            String label = entry.getKey();
                            HashMap<String, String> attribs = entry.getValue();
                           // LOGGER.info("new Vertix pre");
                            VertexSimple v = new VertexSimple();
                            if (root == null) {
                                root = v;
                            }
                            //LOGGER.info("new Vertix done");

                            chunkService.create(v);
                            //LOGGER.info("created Vertix at cs");
                            chunkService.put(v);
                            LOGGER.info("put Vertix at cs");
                            vertex_id_map.put(label, v.getID());
                            vertex_map.put(v.getID(), v);
                            //LOGGER.info("Saved for lookup of vertices");
                        }
                        LOGGER.info(root.toString());
                        LOGGER.info("Parsing edges");
                        for (Map.Entry<String, String[]> entry : edges.entrySet()) {
                            String label = entry.getKey();
                            String[] endPoints = entry.getValue();
                            Edge e = new Edge();
                            if(vertex_id_map.containsKey(endPoints[0])) {
                                Long id1 = vertex_id_map.get(endPoints[0]);
                                e.setFromId(id1);
                            } else {
                                LOGGER.error("Unknown edge "+label+": "+endPoints[0]+"->"+endPoints[1]);
                                continue;
                            }
                            if(vertex_id_map.containsKey(endPoints[1])) {
                                Long id2 = vertex_id_map.get(endPoints[1]);
                                e.setToId(id2);
                            } else {
                                LOGGER.error("Unknown edge "+label+": "+endPoints[0]+"->"+endPoints[1]);
                                continue;
                            }
                            //vertex_map.get(id1).addNeighbour(id2);
                            //chunkService.put(vertex_map.get(id1));
                            //vertex_map.get(id2).addNeighbour(id1);
                            //chunkService.put(vertex_map.get(id2));
                            //LOGGER.info(e.sizeofObject());
                            //LOGGER.info("created edges object");
                            chunkService.create(e);
                            //LOGGER.info("created edge as cs");
                            chunkService.put(e);
                            LOGGER.info("put edges at cs");
                        }

                        LOGGER.info("created all objects");
                        chunkService.get(ll);
                        LOGGER.info(ll.toString());
                        ll.addElement(root.getID());
                        chunkService.put(ll);
                        LOGGER.info("registry updated");
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOGGER.error("Fail");
                        //return -1;
                    }

                }
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
