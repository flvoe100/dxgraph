package de.hhu.bsinfo.dxgraph.algo.bfs;


import de.hhu.bsinfo.dxgraph.algo.bfs.interfaces.TraversalEdgeCallback;
import de.hhu.bsinfo.dxgraph.algo.bfs.interfaces.TraversalVertexCallback;
import de.hhu.bsinfo.dxgraph.data.Edge;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

// TODO make this remote executable by registering traversal callback classes
// and assigning IDs to them for importing/exporting
// TODO this is doing a single threaded traversal for now...have another job that allows spawning further jobs on traversal (?)
// and even move the jobs to be executed on remote nodes where the vertex data is available (?) -> job explosion
// this is a very simple top down only version with a list of next vertices

class TraverseBFSJob extends AbstractJob {
    private static final Logger LOGGER = LogManager.getFormatterLogger(TraverseBFSJob.class.getSimpleName());

    public static final short MS_TYPE_ID = 1;

    static {
        registerType(MS_TYPE_ID, TraverseBFSJob.class);
    }

    private long m_startVertexId = Vertex.INVALID_ID;
    private TraversalVertexCallback m_vertexCallback;
    private TraversalEdgeCallback m_edgeCallback;
    private Class<? extends Vertex> m_vertexClass;
    private Class<? extends Edge> m_edgeClass;

    /**
     * Constructor
     */
    public TraverseBFSJob(final long p_startVertexId, final TraversalVertexCallback p_vertexCallback, final TraversalEdgeCallback p_edgeCallback,
                          final Class<? extends Vertex> p_vertexClass, final Class<? extends Edge> p_edgeClass) {
        m_startVertexId = p_startVertexId;
        m_vertexCallback = p_vertexCallback;
        m_edgeCallback = p_edgeCallback;
        m_vertexClass = p_vertexClass;
        m_edgeClass = p_edgeClass;
    }

    @Override
    public short getTypeID() {
        return MS_TYPE_ID;
    }

    @Override
    protected void execute(final short p_nodeID, final long[] p_chunkIDs) {
        LoggerService logger = getService(LoggerService.class);
        ChunkService chunkService = getService(ChunkService.class);

        // #if LOGGER >= DEBUG
        LOGGER.debug("Starting BFS traversal at 0x%X", m_startVertexId);
        // #endif /* LOGGER >= DEBUG */

        int depth = 0;
        ArrayList<Vertex> current = new ArrayList<>();
        ArrayList<Vertex> next = new ArrayList<>();

        // get root vertex
        try {
            Vertex rootVertex = m_vertexClass.newInstance();
            rootVertex.setID(m_startVertexId);
            if (!chunkService.get().get(rootVertex)) {
                return;
            }
            current.add(rootVertex);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        do {
            depth++;

            for (Vertex vertex : current) {
                if (!evaluateVertex(vertex, depth)) {
                    break;
                }
            }

            for (Vertex vertex : current) {
                Vertex[] neighbors;

                if (vertex.areNeighborsEdgeObjects()) {
                    Edge[] edges = new Edge[vertex.getNeighborCount()];
                    for (int i = 0; i < edges.length; i++) {
                        edges[i].setID(vertex.getNeighbours()[i]);
                    }

                    chunkService.get().get((AbstractChunk[]) edges);

                    for (Edge edge : edges) {
                        if (!evaluateEdge(edge, depth)) {
                            break;
                        }
                    }

                    neighbors = new Vertex[edges.length];
                    for (int i = 0; i < neighbors.length; i++) {
                        neighbors[i].setID(edges[i].getToId());
                    }

                } else {
                    neighbors = new Vertex[vertex.getNeighborCount()];
                    for (int i = 0; i < neighbors.length; i++) {
                        neighbors[i].setID(vertex.getNeighbours()[i]);
                    }
                }

                chunkService.get().get((AbstractChunk[]) neighbors);
                for (Vertex neighbor : neighbors) {
                    if (neighbor.getID() != Vertex.INVALID_ID) {
                        next.add(neighbor);
                    }
                }
            }

        } while (!next.isEmpty());

        // TODO resulting depth? have feature to return result values from jobs
    }

    // -------------------------------------------------------------------

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);
        // TODO
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);
        // TODO
    }

    @Override
    public int sizeofObject() {
        // TODO
        return super.sizeofObject();
    }

    // -------------------------------------------------------------------

    private boolean evaluateVertex(final Vertex p_vertex, final int p_depth) {
        if (m_vertexCallback != null) {
            return m_vertexCallback.evaluateVertex(p_vertex, p_depth);
        }

        return true;
    }

    private boolean evaluateEdge(final Edge p_edge, final int p_depth) {
        if (m_edgeCallback != null) {
            return m_edgeCallback.evaluateEdge(p_edge, p_depth);
        }

        return true;
    }
}