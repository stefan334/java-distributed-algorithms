package com.distributed.pl;

import amcds.pb.AmcdsProto.*;

import com.distributed.tcp.Tcp;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;
import com.distributed.utils.Log;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;


import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * PerfectLink abstraction: wraps raw network messages into PL_DELIVER,
 * and sends PL_SEND as NETWORK_MESSAGE via a hub or direct host:port.
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
        this.host       = host;
        this.port       = port;
        this.hubAddress = hubAddress;
    }

    public static PerfectLink create(String host, int port, String hubAddress) {
        return new PerfectLink(host, port, hubAddress);
    }


    public PerfectLink createWithProps(String systemId,
                                       BlockingQueue<Message> msgQueue,
                                       List<ProcessId> processes) {
        this.systemId  = systemId;
        this.msgQueue  = msgQueue;
        this.processes = processes;
        return this;
    }

    /**
     * Copy this PL with a new parent abstraction id.
     * @param parentAbstraction the abstraction above PL
     * @return a shallow copy with parentId set
     */
    public PerfectLink createCopyWithParentId(String parentAbstraction) {
        PerfectLink copy = new PerfectLink(this.host, this.port, this.hubAddress);
        copy.systemId   = this.systemId;
        copy.msgQueue   = this.msgQueue;
        copy.processes  = this.processes;
        copy.parentId   = parentAbstraction;
        return copy;
    }

    /**
     * Handle an incoming Message from the BUS or upper layer.
     * Transforms NETWORK_MESSAGE → PL_DELIVER, or calls send() for PL_SEND.
     */
    @Override
    public void handle(Message m) throws Exception {
        switch (m.getType()) {
            case NETWORK_MESSAGE:
                NetworkMessage net = m.getNetworkMessage();

                ProcessId sender = null;
                for (ProcessId p : processes) {
                    if (p.getHost().equals(net.getSenderHost())
                            && p.getPort() == net.getSenderListeningPort()) {
                        sender = p;
                        break;
                    }
                }

                PlDeliver.Builder pdBuilder = PlDeliver.newBuilder()
                        .setMessage(net.getMessage());
                if (sender != null) {
                    pdBuilder.setSender(sender);
                } else {
                    Log.debug("PL: unknown sender {}:{}",
                            net.getSenderHost(), net.getSenderListeningPort());
                }

                Message deliver = Message.newBuilder()
                        .setSystemId(m.getSystemId())
                        .setFromAbstractionId(m.getToAbstractionId())
                        .setToAbstractionId(parentId)
                        .setType(Message.Type.PL_DELIVER)
                        .setPlDeliver(pdBuilder.build())
                        .build();

                msgQueue.offer(deliver);
                break;


            case PL_SEND:
                send(m);
                break;

            default:
                throw new UnsupportedOperationException("message not supported");
        }
    }

    /**
     * Sends a PL_SEND message as a NETWORK_MESSAGE over TCP.
     */
    public void send(Message m) throws Exception {
        Log.debug("PLSEND %s", m);

        NetworkMessage.Builder nm = NetworkMessage.newBuilder()
                .setMessage(m.getPlSend().getMessage())
                .setSenderHost(host)
                .setSenderListeningPort(port);

        Message msgToSend = Message.newBuilder()
                .setSystemId(systemId)
                .setToAbstractionId(m.getToAbstractionId())
                .setType(Message.Type.NETWORK_MESSAGE)
                .setNetworkMessage(nm.build())
                .build();
        int size = msgToSend.getSerializedSize();
        byte[] data = new byte[size];
        CodedOutputStream cos = CodedOutputStream.newInstance(data);
        msgToSend.writeTo(cos);
        cos.checkNoSpaceLeft();



        String address = hubAddress;
        if (m.getPlSend().hasDestination()) {
            ProcessId dest = m.getPlSend().getDestination();
            address = Utils.joinHostPort(dest.getHost(), dest.getPort());
        }

        Tcp.send(address, data);
    }


    public Message parse(byte[] data) throws InvalidProtocolBufferException {
        Message msg = Message.parseFrom(data);
        Log.debug("PARSE MSG %s", msg);
        return msg;
    }

    public void destroy() {
    }
}
