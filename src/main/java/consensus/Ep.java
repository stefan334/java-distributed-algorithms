package consensus;


import amcds.pb.AmcdsProto.*;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Ep implements Abstraction {
    private final String id;
    private final String parentId;
    private final BlockingQueue<Message> msgQueue;
    private final List<ProcessId> processes;

    private boolean aborted;
    private final int ets;
    private EpState state;
    private Value tmpVal;
    private final Map<String, EpState> states = new HashMap<>();
    private int accepted;

    public Ep(String parentAbstraction,
              String abstractionId,
              BlockingQueue<Message> msgQueue,
              List<ProcessId> processes,
              int ets,
              EpState initialState) {
        this.id         = abstractionId;
        this.parentId   = parentAbstraction;
        this.msgQueue   = msgQueue;
        this.processes  = processes;
        this.aborted    = false;
        this.ets        = ets;
        this.state      = initialState;
        this.tmpVal     = Value.getDefaultInstance();
        this.accepted   = 0;
    }

    @Override
    public void handle(Message m) {
        String systemId = m.getSystemId();
        if (aborted) return;

        switch (m.getType()) {
            case EP_ABORT:
                // send EpAborted to parent
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.EP_ABORTED)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(parentId)
                                .setEpAborted(
                                        EpAborted.newBuilder()
                                                .setEts(ets)
                                                .setValueTimestamp(state.getValTs())
                                                .setValue(state.getValue())
                                                .build()
                                )
                                .build()
                );
                aborted = true;
                break;

            case EP_PROPOSE:
                tmpVal = m.getEpPropose().getValue();
                // broadcast EP_INTERNAL_READ
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.BEB_BROADCAST)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id + ".beb")
                                .setBebBroadcast(
                                        BebBroadcast.newBuilder()
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Message.Type.EP_INTERNAL_READ)
                                                                .setSystemId(systemId)
                                                                .setFromAbstractionId(id)
                                                                .setToAbstractionId(id)
                                                                .setEpInternalRead(EpInternalRead.getDefaultInstance())
                                                )
                                )
                                .build()
                );
                break;

            case BEB_DELIVER:
                Message inner = m.getBebDeliver().getMessage();
                switch (inner.getType()) {
                    case EP_INTERNAL_READ:
                        // reply EP_INTERNAL_STATE to sender
                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.PL_SEND)
                                        .setSystemId(systemId)
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id + ".pl")
                                        .setPlSend(
                                                PlSend.newBuilder()
                                                        .setDestination(m.getBebDeliver().getSender())
                                                        .setMessage(
                                                                Message.newBuilder()
                                                                        .setType(Message.Type.EP_INTERNAL_STATE)
                                                                        .setSystemId(systemId)
                                                                        .setFromAbstractionId(id)
                                                                        .setToAbstractionId(id)
                                                                        .setEpInternalState(
                                                                                EpInternalState.newBuilder()
                                                                                        .setValueTimestamp(state.getValTs())
                                                                                        .setValue(state.getValue())
                                                                        )
                                                        )
                                        )
                                        .build()
                        );
                        break;

                    case EP_INTERNAL_WRITE:
                        // update local state, then ACK
                        state = new EpState(ets, inner.getEpInternalWrite().getValue());
                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.PL_SEND)
                                        .setSystemId(systemId)
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id + ".pl")
                                        .setPlSend(
                                                PlSend.newBuilder()
                                                        .setDestination(m.getBebDeliver().getSender())
                                                        .setMessage(
                                                                Message.newBuilder()
                                                                        .setType(Message.Type.EP_INTERNAL_ACCEPT)
                                                                        .setSystemId(systemId)
                                                                        .setFromAbstractionId(id)
                                                                        .setToAbstractionId(id)
                                                                        .setEpInternalAccept(EpInternalAccept.getDefaultInstance())
                                                        )
                                        )
                                        .build()
                        );
                        break;

                    case EP_INTERNAL_DECIDED:
                        // notify parent with EP_DECIDE
                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.EP_DECIDE)
                                        .setSystemId(systemId)
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(parentId)
                                        .setEpDecide(
                                                EpDecide.newBuilder()
                                                        .setEts(ets)
                                                        .setValue(state.getValue())
                                        )
                                        .build()
                        );
                        break;

                    default:
                        throw new UnsupportedOperationException(
                                "ep beb deliver message not supported: " + inner.getType());
                }
                break;

            case PL_DELIVER:
                Message plInner = m.getPlDeliver().getMessage();
                switch (plInner.getType()) {
                    case EP_INTERNAL_STATE:
                        // collect state
                        states.put(
                                Utils.getProcessKey(m.getPlDeliver().getSender()),
                                new EpState(
                                        plInner.getEpInternalState().getValueTimestamp(),
                                        plInner.getEpInternalState().getValue()
                                )
                        );
                        // once majority, broadcast EP_INTERNAL_WRITE
                        if (states.size() > processes.size()/2) {
                            // pick highest state
                            EpState highest = states.values().stream()
                                    .max((a,b) -> Integer.compare(a.getValTs(), b.getValTs()))
                                    .orElse(state);
                            if (highest.getValue()!=null && highest.getValue().getDefined()) {
                                tmpVal = highest.getValue();
                            }
                            states.clear();

                            msgQueue.offer(
                                    Message.newBuilder()
                                            .setType(Message.Type.BEB_BROADCAST)
                                            .setSystemId(systemId)
                                            .setFromAbstractionId(id)
                                            .setToAbstractionId(id + ".beb")
                                            .setBebBroadcast(
                                                    BebBroadcast.newBuilder()
                                                            .setMessage(
                                                                    Message.newBuilder()
                                                                            .setType(Message.Type.EP_INTERNAL_WRITE)
                                                                            .setSystemId(systemId)
                                                                            .setFromAbstractionId(id)
                                                                            .setToAbstractionId(id)
                                                                            .setEpInternalWrite(
                                                                                    EpInternalWrite.newBuilder()
                                                                                            .setValue(tmpVal)
                                                                            )
                                                            )
                                            )
                                            .build()
                            );
                        }
                        break;

                    case EP_INTERNAL_ACCEPT:
                        accepted++;
                        // once majority, broadcast EP_INTERNAL_DECIDED
                        if (accepted > processes.size()/2) {
                            accepted = 0;
                            msgQueue.offer(
                                    Message.newBuilder()
                                            .setType(Message.Type.BEB_BROADCAST)
                                            .setSystemId(systemId)
                                            .setFromAbstractionId(id)
                                            .setToAbstractionId(id + ".beb")
                                            .setBebBroadcast(
                                                    BebBroadcast.newBuilder()
                                                            .setMessage(
                                                                    Message.newBuilder()
                                                                            .setType(Message.Type.EP_INTERNAL_DECIDED)
                                                                            .setSystemId(systemId)
                                                                            .setFromAbstractionId(id)
                                                                            .setToAbstractionId(id)
                                                                            .setEpInternalDecided(
                                                                                    EpInternalDecided.newBuilder()
                                                                                            .setValue(tmpVal)
                                                                            )
                                                            )
                                            )
                                            .build()
                            );
                        }
                        break;

                    default:
                        throw new UnsupportedOperationException(
                                "ep pl deliver message not supported: " + plInner.getType());
                }
                break;

            default:
                throw new UnsupportedOperationException("ep message not supported: " + m.getType());
        }
    }

    @Override
    public void destroy() {
        // no cleanup
    }

    /** Helper class to hold timestamp+value */
    public static class EpState {
        private final int valTs;
        private final Value value;
        public EpState(int valTs, Value value) {
            this.valTs = valTs;
            this.value = value;
        }
        public int getValTs() { return valTs; }
        public Value getValue() { return value; }
    }
}
