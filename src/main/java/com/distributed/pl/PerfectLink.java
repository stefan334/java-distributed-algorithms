package com.distributed.pl;

import amcds.pb.AmcdsProto.*;
import com.distributed.tcp.Tcp;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;
import com.distributed.utils.Log;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

/**
 * PerfectLink abstraction: wraps raw network messages into PL_DELIVER,
 * and sends PL_SEND as NETWORK_MESSAGE via a hub or direct host:port.
 * Now includes systemId and messageUuid metadata and manual big-endian length prefix.
 */
public class PerfectLink implements Abstraction {
    private final String host;
    private final int port;
    private final String hubAddress;

    private String systemId;
    private String parentId;
    private BlockingQueue<Message> msgQueue;
    private List<ProcessId> processes;

    private PerfectLink(String host, int port, String hubAddress) {
        this.host = host;
        this.port = port;
        this.hubAddress = hubAddress;
    }

    public static PerfectLink create(String host, int port, String hubAddress) {
        return new PerfectLink(host, port, hubAddress);
    }

    public PerfectLink createWithProps(String systemId,
                                       BlockingQueue<Message> msgQueue,
                                       List<ProcessId> processes) {
        this.systemId = systemId;
        this.msgQueue = msgQueue;
        this.processes = processes;
        return this;
    }

    public PerfectLink createCopyWithParentId(String parentAbstraction) {
        PerfectLink copy = new PerfectLink(host, port, hubAddress);
        copy.systemId = this.systemId;
        copy.msgQueue = this.msgQueue;
        copy.processes = this.processes;
        copy.parentId = parentAbstraction;
        return copy;
    }

    @Override
    public void handle(Message m) throws Exception {
        switch (m.getType()) {
            case NETWORK_MESSAGE:
                // locate sender by matching host+port
                ProcessId sender = null;
                for (ProcessId p : processes) {
                    if (p.getHost().equals(m.getNetworkMessage().getSenderHost())
                            && p.getPort() == m.getNetworkMessage().getSenderListeningPort()) {
                        sender = p;
                        break;
                    }
                }
                // build PL_DELIVER with optional sender
                PlDeliver.Builder pdBuilder = PlDeliver.newBuilder()
                        .setMessage(m.getNetworkMessage().getMessage());
                if (sender != null) {
                    pdBuilder.setSender(sender);
                } else {
                    Log.debug("PL_DELIVER received from unknown sender {}:{}",
                            m.getNetworkMessage().getSenderHost(), m.getNetworkMessage().getSenderListeningPort());
                }
                Message deliver = Message.newBuilder()
                        .setType(Message.Type.PL_DELIVER)
                        .setSystemId(m.getSystemId())
                        .setFromAbstractionId(m.getToAbstractionId())
                        .setToAbstractionId(parentId)
                        .setPlDeliver(pdBuilder.build())
                        .build();
                msgQueue.offer(deliver);
                break;

            case PL_SEND:
                send(m);
                break;

            default:
                throw new UnsupportedOperationException("message not supported: " + m.getType());
        }
    }

    private void send(Message m) throws Exception {
        Message networkMsg = Message.newBuilder()
                .setType(Message.Type.NETWORK_MESSAGE)
                .setSystemId(systemId)
                .setFromAbstractionId(parentId + ".pl")
                .setToAbstractionId(m.getToAbstractionId())
                .setMessageUuid(UUID.randomUUID().toString())
                .setNetworkMessage(NetworkMessage.newBuilder()
                        .setMessage(m.getPlSend().getMessage())
                        .setSenderHost(host)
                        .setSenderListeningPort(port)
                        .build())
                .build();

        byte[] payload = networkMsg.toByteArray();

        String address = hubAddress;
        if (m.getPlSend().hasDestination()) {
            ProcessId dest = m.getPlSend().getDestination();
            address = Utils.joinHostPort(dest.getHost(), dest.getPort());
        }
        Tcp.send(address, payload);
    }

    public Message parse(byte[] data) throws InvalidProtocolBufferException {
        Message msg = Message.parseFrom(data);
        Log.debug("PARSE MSG %s", msg);
        return msg;
    }

    public void destroy() {}
}
