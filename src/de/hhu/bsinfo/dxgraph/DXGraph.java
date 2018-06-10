/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxgraph;

import java.lang.reflect.Array;
import java.util.ArrayList;

import de.hhu.bsinfo.dxgraph.algo.bfs.interfaces.TraversalVertexCallback;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxgraph.data.Edge;
import de.hhu.bsinfo.dxgraph.data.Vertex;
import de.hhu.bsinfo.dxram.chunk.ChunkRemoveService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Special wrapper API (though DXRAMEngine is still accessible) providing
 * graph processing and analysis related functions. This also simplifies
 * or wraps access to certain services to create a common API for graph related
 * tasks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public class DXGraph extends AbstractApplication {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXGraph.class.getSimpleName());

    private ChunkService m_chunkService;
    private ChunkRemoveService m_chunkRemoveService;

    /**
     * Create storage on the current node for one or multiple vertices. This assigns
     * a valid ID to each successfully created vertex. The actual
     * data stored with the vertex is not stored with this call.
     *
     * @param p_vertices
     *         Vertices to create storage space for.
     */
    public void createVertices(final Vertex... p_vertices) {
        m_chunkService.create((DataStructure[]) p_vertices);
    }

    /**
     * Create storage on a remote node for one or multiple vertices. This assigns
     * a valid ID to each successfully created vertex. The actual
     * data stored with the vertex is not stored with this call.
     *
     * @param p_nodeId
     *         Node id of another peer to allocate the space on.
     * @param p_vertices
     *         Vertices to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public int createVertices(final short p_nodeId, final Vertex... p_vertices) {
        return m_chunkService.createRemote(p_nodeId, (DataStructure[]) p_vertices);
    }

    /**
     * Create storage on the current node for one or multiple edges. This assigns
     * a valid ID to each successfully created edge. The actual
     * data stored with the edge is not stored with this call.
     *
     * @param p_edges
     *         Edges to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public void createEdges(final Edge... p_edges) {
        m_chunkService.create((DataStructure[]) p_edges);
    }

    /**
     * Create storage on a remote node for one or multiple edges. This assigns
     * a valid ID to each successfully created edge. The actual
     * data stored with the edge is not stored with this call.
     *
     * @param p_nodeId
     *         Node id of another peer to allocate the space on.
     * @param p_edges
     *         VerticEdgeses to create storage space for.
     * @return Number of successfully created storage locations.
     */
    public int createEdges(final short p_nodeId, final Edge... p_edges) {
        return m_chunkService.createRemote(p_nodeId, (DataStructure[]) p_edges);
    }

    /**
     * Write the data of one or multiple vertices to its storage location(s).
     *
     * @param p_vertices
     *         Vertices to write the data to the storage.
     * @return Number of successfully written vertices.
     */
    public int putVertices(final Vertex... p_vertices) {
        return m_chunkService.put((DataStructure[]) p_vertices);
    }

    /**
     * Write the data of one or multiple dges to its storage location(s).
     *
     * @param p_edges
     *         Edges to write the data to the storage.
     * @return Number of successfully written edges.
     */
    public int putEdges(final Edge... p_edges) {
        return m_chunkService.put((DataStructure[]) p_edges);
    }

    /**
     * Read the data of one or multiple vertices from its storage location(s).
     *
     * @param p_vertices
     *         Vertices to read the data from the storage.
     * @return Number of successfully read vertices.
     */
    public int getVertices(final Vertex... p_vertices) {
        return m_chunkService.get((DataStructure[]) p_vertices);
    }

    /**
     * Read the data of one or multiple edges from its storage location(s).
     *
     * @param p_edges
     *         Edges to read the data from the storage.
     * @return Number of successfully read edges.
     */
    public int getEdges(final Edge... p_edges) {
        return m_chunkService.get((DataStructure[]) p_edges);
    }

    /**
     * Delete one or multiple stored vertices from the storage.
     *
     * @param p_vertices
     *         Vertices to delete from storage.
     * @return Number of successfully deleted vertices.
     */
    public int deleteVertices(final Vertex... p_vertices) {
        return m_chunkRemoveService.remove((DataStructure[]) p_vertices);
    }

    /**
     * Delete one or multiple stored edges from the storage.
     *
     * @param p_edges
     *         Edges to delete from storage.
     * @return Number of successfully deleted edges.
     */
    public int deleteEdges(final Edge... p_edges) {
        return m_chunkRemoveService.remove((DataStructure[]) p_edges);
    }

    /**
     * Scan the vertex by getting all its edge objects it is connected to.
     * If the edges are not edge objects but direct connections to the neighbor
     * vertex, this call fails.
     *
     * @param p_vertex
     *         Vertex to scan. If invalid, null is returned.
     * @param p_edgeClass
     *         Class of the edges to return.
     * @param <T>
     *         Type of the edges to create instances of.
     * @return Edge objects of the scanned vertex with their data read from the storage.
     */
    public <T extends Edge> T[] scanEdges(final Vertex p_vertex, final Class<T> p_edgeClass) {
        if (p_vertex.getID() == Vertex.INVALID_ID) {
            return null;
        }

        if (!p_vertex.areNeighborsEdgeObjects()) {
            return null;
        }

        T[] edges = (T[]) Array.newInstance(p_edgeClass, p_vertex.getNeighborCount());
        for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
            try {
                edges[i] = p_edgeClass.newInstance();
                edges[i].setID(p_vertex.getNeighbours()[i]);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        m_chunkService.get((DataStructure[]) edges);
        return edges;
    }

    /**
     * Scan the vertex by getting all its neighbor vertices it is connected to.
     * If the edges are actual objects, they are read but skipped automatically.
     *
     * @param p_vertex
     *         Vertex to scan. If invalid, null is returned.
     * @param p_vertexClass
     *         Class of the vertex instances to return.
     * @param <T>
     *         Type of the vertex instances to create.
     * @return Neighbor vertex objects of the scanned vertex with their data stored.
     */
    public <T extends Vertex> T[] scanNeighborVertices(final Vertex p_vertex, final Class<T> p_vertexClass) {
        if (p_vertex.getID() == Vertex.INVALID_ID) {
            return null;
        }

        T[] vertices = (T[]) Array.newInstance(p_vertexClass, p_vertex.getNeighborCount());
        if (p_vertex.areNeighborsEdgeObjects()) {
            // read and skip edges
            Edge[] edges = new Edge[p_vertex.getNeighborCount()];
            for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
                edges[i] = new Edge(p_vertex.getNeighbours()[i]);
            }

            m_chunkService.get((DataStructure[]) edges);

            for (int i = 0; i < edges.length; i++) {
                if (edges[i] != null) {
                    try {
                        vertices[i] = p_vertexClass.newInstance();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    vertices[i].setID(edges[i].getToId());
                }
            }
        } else {
            for (int i = 0; i < p_vertex.getNeighborCount(); i++) {
                if (p_vertex.getNeighbours()[i] != Vertex.INVALID_ID) {
                    try {
                        vertices[i] = p_vertexClass.newInstance();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    vertices[i].setID(p_vertex.getNeighbours()[i]);
                }
            }
        }

        m_chunkService.get((DataStructure[]) vertices);
        return vertices;
    }

    public long traverseBFS(final Vertex p_startVertex, final TraversalVertexCallback p_callback) {
        // returns number of vertices traversed
        // TODO using job system here doesn't seem to be a bad idea
        return 0;
    }

    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return DXRAM.VERSION;
    }

    @Override
    public String getApplicationName() {
        return "DXGraph";
    }


    @Override
    public boolean useConfigurationFile() {
        return false;
    }

    @Override
    public void main() {
        m_chunkService = getService(ChunkService.class);
        m_chunkRemoveService = getService(ChunkRemoveService.class);
        LOGGER.info("Started DXGraph 1.0");
    }

    @Override
    public void signalShutdown() {

    }

}
