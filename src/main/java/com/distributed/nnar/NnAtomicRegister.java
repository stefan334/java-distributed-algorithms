package com.distributed.nnar;

import amcds.pb.AmcdsProto.*;
import amcds.pb.AmcdsProto.Message.*;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Log;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;


public class NnAtomicRegister implements Abstraction {
    private final BlockingQueue<Message> msgQueue;
    private final int N;
    private final String key;

    private int timestamp = 0;
    private int writerRank = 0;
    private int value = -1;

    private int acks = 0;
    private Value writeVal;
    private int readId = 0;
    private Map<String, NnarInternalValue> readList = new HashMap<>();
    private boolean reading = false;

    public NnAtomicRegister(BlockingQueue<Message> msgQueue, int N, String key) {
        this.msgQueue = msgQueue;
        this.N        = N;
        this.key      = key;
    }

    @Override
    public void handle(Message m) {
        Log.info("Register handles {}", m);
        Message msgToSend = null;
        String aId = getAbstractionId();

        switch (m.getType()) {
            case BEB_DELIVER:
                BebDeliver bd = m.getBebDeliver();
                Message inner = bd.getMessage();
                switch (inner.getType()) {
                    case NNAR_INTERNAL_READ:
                        int incomingReadId = inner.getNnarInternalRead().getReadId();
                        if (readId == 0) readId = incomingReadId;
                        Log.info("Internal read {}", incomingReadId);

                        msgToSend = Message.newBuilder()
                                .setType(Type.PL_SEND)
                                .setFromAbstractionId(aId)
                                .setToAbstractionId(aId + ".pl")
                                .setSystemId(m.getSystemId())
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setDestination(bd.getSender())
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Type.NNAR_INTERNAL_VALUE)
                                                                .setFromAbstractionId(aId)
                                                                .setToAbstractionId(aId)
                                                                .setSystemId(m.getSystemId())
                                                                .setNnarInternalValue(buildInternalValue(incomingReadId))
                                                                .build()
                                                )
                                                .build()
                                )
                                .build();
                        break;

                    case NNAR_INTERNAL_WRITE:
                        Log.info("Internal write for read {}", inner.getNnarInternalWrite().getReadId());
                        NnarInternalWrite iw = inner.getNnarInternalWrite();
                        NnarInternalValue incomingVal = NnarInternalValue.newBuilder()
                                .setTimestamp(iw.getTimestamp())
                                .setWriterRank(iw.getWriterRank())
                                .build();
                        NnarInternalValue currentVal = NnarInternalValue.newBuilder()
                                .setTimestamp(timestamp)
                                .setWriterRank(writerRank)
                                .build();
                        if (compare(incomingVal, currentVal) > 0) {
                            timestamp   = iw.getTimestamp();
                            writerRank  = iw.getWriterRank();
                            updateValue(iw.getValue());
                        }

                        msgToSend = Message.newBuilder()
                                .setType(Type.PL_SEND)
                                .setFromAbstractionId(aId)
                                .setToAbstractionId(aId + ".pl")
                                .setSystemId(m.getSystemId())
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setDestination(bd.getSender())
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Type.NNAR_INTERNAL_ACK)
                                                                .setFromAbstractionId(aId)
                                                                .setToAbstractionId(aId)
                                                                .setSystemId(m.getSystemId())
                                                                .setNnarInternalAck(
                                                                        NnarInternalAck.newBuilder()
                                                                                .setReadId(readId)
                                                                                .build()
                                                                )
                                                                .build()
                                                )
                                                .build()
                                )
                                .build();
                        break;

                    default:
                        throw new UnsupportedOperationException("message not supported");
                }
                break;

            case NNAR_WRITE:
                readId    = readId + 1;
                writeVal  = m.getNnarWrite().getValue();
                acks      = 0;
                reading   = false;
                readList.clear();
                Log.info("Init write {} with readId {}", writeVal, readId);

                msgToSend = Message.newBuilder()
                        .setType(Type.BEB_BROADCAST)
                        .setFromAbstractionId(aId)
                        .setToAbstractionId(aId + ".beb")
                        .setSystemId(m.getSystemId())
                        .setBebBroadcast(
                                BebBroadcast.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setType(Type.NNAR_INTERNAL_READ)
                                                        .setFromAbstractionId(aId)
                                                        .setToAbstractionId(aId)
                                                        .setSystemId(m.getSystemId())
                                                        .setNnarInternalRead(
                                                                NnarInternalRead.newBuilder()
                                                                        .setReadId(readId)
                                                                        .build()
                                                        )
                                                        .build()
                                        )
                                        .build()
                        )
                        .build();
                break;

            case NNAR_READ:
                readId    = readId + 1;
                acks      = 0;
                reading   = true;
                readList.clear();
                Log.info("Init read with readId {}", readId);

                msgToSend = Message.newBuilder()
                        .setType(Type.BEB_BROADCAST)
                        .setFromAbstractionId(aId)
                        .setToAbstractionId(aId + ".beb")
                        .setSystemId(m.getSystemId())
                        .setBebBroadcast(
                                BebBroadcast.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setType(Type.NNAR_INTERNAL_READ)
                                                        .setFromAbstractionId(aId)
                                                        .setToAbstractionId(aId)
                                                        .setSystemId(m.getSystemId())
                                                        .setNnarInternalRead(
                                                                NnarInternalRead.newBuilder()
                                                                        .setReadId(readId)
                                                                        .build()
                                                        )
                                                        .build()
                                        )
                                        .build()
                        )
                        .build();
                break;

            case PL_DELIVER:
                Message pdMsg = m.getPlDeliver().getMessage();
                switch (pdMsg.getType()) {
                    case NNAR_INTERNAL_VALUE:
                        NnarInternalValue iv = pdMsg.getNnarInternalValue();
                        int incReadId = iv.getReadId();
                        Log.info("NNAR Internal value for read {} (reading={})", incReadId, reading);

                        if (incReadId == readId) {
                            String senderId = m.getPlDeliver().getSender().getOwner()
                                    + m.getPlDeliver().getSender().getIndex();
                            iv = iv.toBuilder()
                                    .setWriterRank(m.getPlDeliver().getSender().getRank())
                                    .build();
                            readList.put(senderId, iv);

                            if (readList.size() > N/2) {
                                NnarInternalValue h = highest();
                                readList.clear();

                                if (!reading) {
                                    h = h.toBuilder()
                                            .setTimestamp(h.getTimestamp() + 1)
                                            .setWriterRank(writerRank)
                                            .setValue(writeVal)
                                            .build();
                                }

                                msgToSend = Message.newBuilder()
                                        .setType(Type.BEB_BROADCAST)
                                        .setFromAbstractionId(aId)
                                        .setToAbstractionId(aId + ".beb")
                                        .setSystemId(m.getSystemId())
                                        .setBebBroadcast(
                                                BebBroadcast.newBuilder()
                                                        .setMessage(
                                                                Message.newBuilder()
                                                                        .setType(Type.NNAR_INTERNAL_WRITE)
                                                                        .setFromAbstractionId(aId)
                                                                        .setToAbstractionId(aId)
                                                                        .setSystemId(m.getSystemId())
                                                                        .setNnarInternalWrite(
                                                                                NnarInternalWrite.newBuilder()
                                                                                        .setReadId(incReadId)
                                                                                        .setTimestamp(h.getTimestamp())
                                                                                        .setWriterRank(h.getWriterRank())
                                                                                        .setValue(h.getValue())
                                                                                        .build()
                                                                        )
                                                                        .build()
                                                        )
                                                        .build()
                                        )
                                        .build();
                            }
                        }
                        break;

                    case NNAR_INTERNAL_ACK:
                        NnarInternalAck na = pdMsg.getNnarInternalAck();
                        int incAckReadId = na.getReadId();
                        Log.info("NNAR Internal ack for read {} (reading={})", incAckReadId, reading);

                        if (incAckReadId == readId) {
                            acks += 1;
                            if (acks > N/2) {
                                acks = 0;
                                if (reading) {
                                    reading = false;
                                    msgToSend = Message.newBuilder()
                                            .setType(Type.NNAR_READ_RETURN)
                                            .setFromAbstractionId(aId)
                                            .setToAbstractionId("app")
                                            .setSystemId(m.getSystemId())
                                            .setNnarReadReturn(
                                                    NnarReadReturn.newBuilder()
                                                            .setValue(buildInternalValue(incAckReadId).getValue())
                                                            .build()
                                            )
                                            .build();
                                } else {
                                    msgToSend = Message.newBuilder()
                                            .setType(Type.NNAR_WRITE_RETURN)
                                            .setFromAbstractionId(aId)
                                            .setToAbstractionId("app")
                                            .setSystemId(m.getSystemId())
                                            .setNnarWriteReturn(NnarWriteReturn.getDefaultInstance())
                                            .build();
                                }
                            }
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException("message not supported");
                }
                break;

            default:
                throw new UnsupportedOperationException("message not supported");
        }

        if (msgToSend != null) {
            msgQueue.offer(msgToSend);
        }
    }

    private NnarInternalValue highest() {
        NnarInternalValue highest = null;
        for (NnarInternalValue v : readList.values()) {
            if (highest == null || compare(v, highest) > 0) {
                highest = v;
            }
        }
        return highest;
    }

    private String getAbstractionId() {
        return "app.nnar[" + key + "]";
    }

    private NnarInternalValue buildInternalValue(int incomingReadId) {
        boolean defined = (value != -1);
        return NnarInternalValue.newBuilder()
                .setReadId(incomingReadId)
                .setTimestamp(timestamp)
                .setWriterRank(writerRank)
                .setValue(
                        Value.newBuilder()
                                .setV(value)
                                .setDefined(defined)
                                .build()
                )
                .build();
    }

    private static int compare(NnarInternalValue v1, NnarInternalValue v2) {
        if (v1.getTimestamp() != v2.getTimestamp()) {
            return Integer.compare(v1.getTimestamp(), v2.getTimestamp());
        }
        return Integer.compare(v1.getWriterRank(), v2.getWriterRank());
    }

    private void updateValue(Value v) {
        if (v.getDefined()) {
            value = v.getV();
        } else {
            value = -1;
        }
    }

    public void destroy() {
    }
}
