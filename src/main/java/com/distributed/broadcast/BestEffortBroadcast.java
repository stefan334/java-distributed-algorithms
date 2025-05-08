package com.distributed.broadcast;

import amcds.pb.AmcdsProto.*;
import com.distributed.utils.Abstraction;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Best‐Effort Broadcast: forwards BEB_BROADCAST to all via PL_SEND,
 * and on PL_DELIVER emits BEB_DELIVER.
 */
public class BestEffortBroadcast implements Abstraction {
    private final BlockingQueue<Message> msgQueue;
    private final List<ProcessId> processes;
    private final String id;

    public BestEffortBroadcast(BlockingQueue<Message> msgQueue,
                               List<ProcessId> processes,
                               String id) {
        this.msgQueue  = msgQueue;
        this.processes = processes;
        this.id        = id;
    }

    /**
     * Handle an incoming message.
     * @param m the message from higher/lower abstraction
     */
    @Override
    public void handle(Message m) {
        Message msgToSend;

        switch (m.getType()) {
            case BEB_BROADCAST:
                BebBroadcast beb = m.getBebBroadcast();
                for (ProcessId p : processes) {
                    msgToSend = Message.newBuilder()
                            .setType(Message.Type.PL_SEND)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id + ".pl")
                            .setPlSend(
                                    PlSend.newBuilder()
                                            .setDestination(p)
                                            .setMessage(beb.getMessage())
                                            .build()
                            )
                            .build();
                    msgQueue.offer(msgToSend);
                }
                break;

            case PL_DELIVER:
                BebDeliver deliver = BebDeliver.newBuilder()
                        .setSender(m.getPlDeliver().getSender())
                        .setMessage(m.getPlDeliver().getMessage())
                        .build();
                msgToSend = Message.newBuilder()
                        .setType(Message.Type.BEB_DELIVER)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(m.getPlDeliver().getMessage().getToAbstractionId())
                        .setBebDeliver(deliver)
                        .build();
                msgQueue.offer(msgToSend);
                break;

            default:
                throw new UnsupportedOperationException("message not supported");
        }
    }

    public void destroy() {
    }
}
