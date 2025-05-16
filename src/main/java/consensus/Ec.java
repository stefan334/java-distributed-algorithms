package consensus;

import amcds.pb.AmcdsProto.*;
import com.distributed.utils.Abstraction;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Ec implements Abstraction {
    private final String id;
    private final String parentId;
    private final ProcessId self;
    private final BlockingQueue<Message> msgQueue;
    private final List<ProcessId> processes;

    private ProcessId trusted;
    private int lastTs;
    private int ts;

    public Ec(String parentAbstraction,
              String abstractionId,
              ProcessId ownProcess,
              BlockingQueue<Message> msgQueue,
              List<ProcessId> processes) {
        this.id        = abstractionId;
        this.parentId  = parentAbstraction;
        this.self      = ownProcess;
        this.msgQueue  = msgQueue;
        this.processes = processes;

        // pick the process with highest rank
        this.trusted = processes.stream()
                .max(Comparator.comparingInt(ProcessId::getRank))
                .orElseThrow(() -> new IllegalArgumentException("Empty process list"));
        this.lastTs = 0;
        this.ts     = ownProcess.getRank();
    }

    @Override
    public void handle(Message m) {
        switch (m.getType()) {
            case ELD_TRUST:
                // update trusted and maybe start epoch if it's me
                this.trusted = m.getEldTrust().getProcess();
                handleSelfTrust(m.getSystemId());
                break;

            case PL_DELIVER:
                // only care about internal-nack here
                if (m.getPlDeliver().getMessage().getType() == Message.Type.EC_INTERNAL_NACK) {
                    handleSelfTrust(m.getSystemId());
                }
                break;

            case BEB_DELIVER:
                Message inner = m.getBebDeliver().getMessage();
                if (inner.getType() == Message.Type.EC_INTERNAL_NEW_EPOCH) {
                    int newTs = inner.getEcInternalNewEpoch().getTimestamp();
                    String lKey = processKey(m.getBebDeliver().getSender());
                    String trustedKey = processKey(this.trusted);

                    if (lKey.equals(trustedKey) && newTs > this.lastTs) {
                        this.lastTs = newTs;
                        // send EC_START_EPOCH to parent
                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.EC_START_EPOCH)
                                        .setSystemId(m.getSystemId())
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(parentId)
                                        .setEcStartEpoch(
                                                EcStartEpoch.newBuilder()
                                                        .setNewTimestamp(newTs)
                                                        .setNewLeader(m.getBebDeliver().getSender())
                                                        .build()
                                        )
                                        .build()
                        );
                    } else {
                        // nack ourselves so we’ll broadcast new epoch later
                        Message nackInner = Message.newBuilder()
                                .setType(Message.Type.EC_INTERNAL_NACK)
                                .setSystemId(m.getSystemId())
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id)
                                .setEcInternalNack(EcInternalNack.getDefaultInstance())
                                .build();

                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.PL_SEND)
                                        .setSystemId(m.getSystemId())
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id + ".pl")
                                        .setPlSend(
                                                PlSend.newBuilder()
                                                        .setMessage(nackInner)
                                                        .build()
                                        )
                                        .build()
                        );
                    }
                } else {
                    throw new UnsupportedOperationException(
                            "ec unknown beb deliver message type: " + inner.getType());
                }
                break;

            default:
                throw new UnsupportedOperationException(
                        "ec unknown message type: " + m.getType());
        }
    }

    private void handleSelfTrust(String systemId) {
        // if I am trusted, start a new epoch
        if (processKey(self).equals(processKey(trusted))) {
            // bump ts by N
            this.ts = lastTs + processes.size();

            // broadcast EC_INTERNAL_NEW_EPOCH on beb
            Message newEpochInner = Message.newBuilder()
                    .setType(Message.Type.EC_INTERNAL_NEW_EPOCH)
                    .setSystemId(systemId)
                    .setFromAbstractionId(id)
                    .setToAbstractionId(id)
                    .setEcInternalNewEpoch(
                            EcInternalNewEpoch.newBuilder()
                                    .setTimestamp(ts)
                                    .build()
                    )
                    .build();

            msgQueue.offer(
                    Message.newBuilder()
                            .setType(Message.Type.BEB_BROADCAST)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id + ".beb")
                            .setBebBroadcast(
                                    BebBroadcast.newBuilder()
                                            .setMessage(newEpochInner)
                                            .build()
                            )
                            .build()
            );
        }
    }

    private String processKey(ProcessId p) {
        // mimic Go’s utils.GetProcessKey: owner+index
        return p.getOwner() + "-" + p.getIndex();
    }

    @Override
    public void destroy() {
        // no resources to clean up
    }
}
