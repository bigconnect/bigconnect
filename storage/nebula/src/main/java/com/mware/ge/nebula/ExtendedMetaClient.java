package com.mware.ge.nebula;

import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TCompactProtocol;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TTransportException;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import com.vesoft.nebula.ErrorCode;
import com.vesoft.nebula.HostAddr;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.meta.AbstractMetaClient;
import com.vesoft.nebula.client.meta.exception.ExecuteFailedException;
import com.vesoft.nebula.meta.*;

import java.util.*;

public class ExtendedMetaClient extends AbstractMetaClient {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(ExtendedMetaClient.class);

    private static final int RETRY_TIMES = 1;

    public static final int LATEST_SCHEMA_VERSION = -1;

    private static final int DEFAULT_TIMEOUT_MS = 1000;
    private static final int DEFAULT_CONNECTION_RETRY_SIZE = 3;
    private static final int DEFAULT_EXECUTION_RETRY_SIZE = 3;

    private MetaService.Client client;
    private final List<HostAddress> addresses;

    public ExtendedMetaClient(String host, int port) {
        this(new HostAddress(host, port));
    }

    public ExtendedMetaClient(HostAddress address) {
        this(Arrays.asList(address), DEFAULT_CONNECTION_RETRY_SIZE, DEFAULT_EXECUTION_RETRY_SIZE);
    }

    public ExtendedMetaClient(List<HostAddress> addresses) {
        this(addresses, DEFAULT_CONNECTION_RETRY_SIZE, DEFAULT_EXECUTION_RETRY_SIZE);
    }

    public ExtendedMetaClient(List<HostAddress> addresses, int connectionRetry, int executionRetry) {
        this(addresses, DEFAULT_TIMEOUT_MS, connectionRetry, executionRetry);
    }

    public ExtendedMetaClient(List<HostAddress> addresses, int timeout, int connectionRetry,
                              int executionRetry) {
        super(addresses, timeout, connectionRetry, executionRetry);
        this.addresses = addresses;
    }

    public void connect() throws TException {
        doConnect();
    }

    /**
     * connect nebula meta server
     */
    private void doConnect() throws TTransportException {
        Random random = new Random(System.currentTimeMillis());
        int position = random.nextInt(addresses.size());
        HostAddress address = addresses.get(position);
        getClient(address.getHost(), address.getPort());
    }

    private void getClient(String host, int port) throws TTransportException {
        transport = new TSocket(host, port, timeout, timeout);
        transport.open();
        protocol = new TCompactProtocol(transport);
        client = new MetaService.Client(protocol);
    }

    private void freshClient(HostAddr leader) throws TTransportException {
        close();
        getClient(leader.getHost(), leader.getPort());
    }

    /**
     * close transport
     */
    public void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }

    /**
     * get all spaces
     *
     * @return
     */
    public synchronized List<IdName> getSpaces() throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;
        ListSpacesReq request = new ListSpacesReq();
        ListSpacesResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.listSpaces(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("List Spaces Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSpaces();
        } else {
            LOGGER.error("Get Spaces execute failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get Spaces execute failed, errorCode: " + response.getCode());
        }
    }

    /**
     * get one space
     *
     * @param spaceName nebula graph space
     * @return SpaceItem
     */
    public synchronized SpaceItem getSpace(String spaceName) throws TException,
            ExecuteFailedException {
        int retry = RETRY_TIMES;
        GetSpaceReq request = new GetSpaceReq();
        request.setSpace_name(spaceName.getBytes());
        GetSpaceResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.getSpace(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Space Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getItem();
        } else {
            LOGGER.error("Get Space execute failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get Space execute failed, errorCode: " + response.getCode());
        }
    }


    public synchronized int createSpace(String spaceName, int partitions, int replicas) throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;

        CreateSpaceReq request = new CreateSpaceReq(new SpaceDesc(
                spaceName.getBytes(),
                partitions,
                replicas,
                "utf8".getBytes(),
                "utf8_bin".getBytes(),
                null
        ), true);

        ExecResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.createSpace(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() != ErrorCode.SUCCEEDED) {
            LOGGER.error("Create space failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Create space execute failed, errorCode: " + response.getCode());
        } else {
            return response.getId().getSpace_id();
        }
    }

    public synchronized void dropSpace(String spaceName) throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;

        DropSpaceReq request = new DropSpaceReq(
                spaceName.getBytes(),
                true
        );
        ExecResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.dropSpace(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() != ErrorCode.SUCCEEDED) {
            LOGGER.error("Drop space failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Drop space execute failed, errorCode: " + response.getCode());
        }
    }

    /**
     * get all tags of spaceName
     *
     * @param spaceName nebula graph space
     * @return TagItem list
     */
    public synchronized List<TagItem> getTags(String spaceName)
            throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;

        int spaceID = getSpace(spaceName).space_id;
        ListTagsReq request = new ListTagsReq(spaceID);
        ListTagsResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.listTags(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getTags();
        } else {
            LOGGER.error("Get tags execute failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get Tags execute failed, errorCode: " + response.getCode());
        }
    }


    /**
     * get schema of specific tag
     *
     * @param spaceName nebula graph space
     * @param tagName   nebula tag name
     * @return Schema
     */
    public synchronized Schema getTag(String spaceName, String tagName)
            throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;
        GetTagReq request = new GetTagReq();
        int spaceID = getSpace(spaceName).getSpace_id();
        request.setSpace_id(spaceID);
        request.setTag_name(tagName.getBytes());
        request.setVersion(LATEST_SCHEMA_VERSION);
        GetTagResp response = null;

        try {
            while (retry-- >= 0) {
                response = client.getTag(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Tag Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSchema();
        } else {
            LOGGER.error("Get tag execute failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get tag execute failed, errorCode: " + response.getCode());
        }
    }

    public synchronized void createTag(int spaceId, String tagName, Schema schema) throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;

        CreateTagReq request = new CreateTagReq(
                spaceId, tagName.getBytes(), schema, true
        );

        ExecResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.createTag(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Create Tag Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() != ErrorCode.SUCCEEDED) {
            LOGGER.error("Create tag failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Create tag execute failed, errorCode: " + response.getCode());
        }
    }


    /**
     * get all edges of specific space
     *
     * @param spaceName nebula graph space
     * @return EdgeItem list
     */
    public synchronized List<EdgeItem> getEdges(String spaceName)
            throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;
        int spaceID = getSpace(spaceName).getSpace_id();
        ListEdgesReq request = new ListEdgesReq(spaceID);
        ListEdgesResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.listEdges(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Edge Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getEdges();
        } else {
            LOGGER.error("Get edges execute failed: errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get execute edges failed, errorCode: " + response.getCode());
        }
    }

    /**
     * get schema of specific edgeRow
     *
     * @param spaceName nebula graph space
     * @param edgeName  nebula edgeRow name
     * @return Schema
     */
    public synchronized Schema getEdge(String spaceName, String edgeName)
            throws TException, ExecuteFailedException {
        int retry = RETRY_TIMES;
        GetEdgeReq request = new GetEdgeReq();
        int spaceID = getSpace(spaceName).getSpace_id();
        request.setSpace_id(spaceID);
        request.setEdge_name(edgeName.getBytes());
        request.setVersion(LATEST_SCHEMA_VERSION);
        GetEdgeResp response = null;

        try {
            while (retry-- >= 0) {
                response = client.getEdge(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Edge Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getSchema();
        } else {
            LOGGER.error("Get Edge execute failed, errorCode: " + response.getCode());
            throw new ExecuteFailedException(
                    "Get Edge execute failed, errorCode: " + response.getCode());
        }
    }


    /**
     * Get all parts and the address in a space
     * Store in this.parts
     *
     * @param spaceName Nebula space name
     * @return
     */
    public synchronized Map<Integer, List<HostAddr>> getPartsAlloc(String spaceName)
            throws ExecuteFailedException, TException {
        int retry = RETRY_TIMES;
        GetPartsAllocReq request = new GetPartsAllocReq();
        int spaceID = getSpace(spaceName).getSpace_id();
        request.setSpace_id(spaceID);

        GetPartsAllocResp response = null;
        try {
            while (retry-- >= 0) {
                response = client.getPartsAlloc(request);
                if (response.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(response.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error(String.format("Get Parts Error: %s", e.getMessage()));
            throw e;
        }
        if (response.getCode() == ErrorCode.SUCCEEDED) {
            return response.getParts();
        } else {
            LOGGER.error("Get Parts execute failed, errorCode" + response.getCode());
            throw new ExecuteFailedException(
                    "Get Parts execute failed, errorCode" + response.getCode());
        }
    }

    /**
     * get all Storaged servers
     */
    public synchronized Set<HostAddr> listHosts() {
        int retry = RETRY_TIMES;
        ListHostsReq request = new ListHostsReq();
        request.setType(ListHostType.STORAGE);
        ListHostsResp resp = null;
        try {
            while (retry-- >= 0) {
                resp = client.listHosts(request);
                if (resp.getCode() == ErrorCode.E_LEADER_CHANGED) {
                    freshClient(resp.getLeader());
                } else {
                    break;
                }
            }
        } catch (TException e) {
            LOGGER.error("listHosts error", e);
            return null;
        }
        if (resp.getCode() != ErrorCode.SUCCEEDED) {
            LOGGER.error("listHosts execute failed, errorCode: " + resp.getCode());
            return null;
        }
        Set<HostAddr> hostAddrs = new HashSet<>();
        for (HostItem hostItem : resp.hosts) {
            if (hostItem.getStatus().getValue() == HostStatus.ONLINE.getValue()) {
                hostAddrs.add(hostItem.getHostAddr());
            }
        }
        return hostAddrs;
    }
}
