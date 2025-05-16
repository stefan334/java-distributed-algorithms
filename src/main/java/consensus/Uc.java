package consensus;


import amcds.pb.AmcdsProto.*;
import com.distributed.broadcast.BestEffortBroadcast;
import com.distributed.pl.PerfectLink;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Uc implements Abstraction {
    private final String id;
    private final BlockingQueue<Message> msgQueue;
    private final java.util.Map<String, Abstraction> abstractions;
    private final List<ProcessId> processes;
    private final ProcessId self;
    private final PerfectLink pl;

    private Value val;
    private boolean proposed;
    private boolean decided;
    private int ets;
    private ProcessId l;
    private int newTs;
    private ProcessId newL;

    public Uc(String id,
              BlockingQueue<Message> msgQueue,
              java.util.Map<String, Abstraction> abstractions,
              List<ProcessId> processes,
              ProcessId ownProcess,
              PerfectLink pl) {
        this.id           = id;
        this.msgQueue     = msgQueue;
        this.abstractions = abstractions;
        this.processes    = processes;
        this.self         = ownProcess;
        this.pl           = pl;

        this.val      = Value.getDefaultInstance();
        this.proposed = false;
        this.decided  = false;
        this.ets      = 0;
        // initial leader = max‐rank in processes
        this.l        = processes.stream()
                .max(Utils.byRank())
                .orElseThrow();
        this.newTs    = 0;
        this.newL     = ProcessId.getDefaultInstance();

        // install first EP
        addEpAbstractions(new Ep.EpState(0, this.val));
    }

    @Override
    public void handle(Message m) {
        String systemId = m.getSystemId();
        switch (m.getType()) {
            case UC_PROPOSE:
                this.val = m.getUcPropose().getValue();
                break;

            case EC_START_EPOCH:
                this.newTs = m.getEcStartEpoch().getNewTimestamp();
                this.newL  = m.getEcStartEpoch().getNewLeader();

                // abort current epoch
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.EP_ABORT)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id + getEpId())
                                .setEpAbort(EpAbort.getDefaultInstance())
                                .build()
                );
                break;

            case EP_ABORTED:
                // only react if it matches our current ets
                if (this.ets == m.getEpAborted().getEts()) {
                    this.ets      = this.newTs;
                    this.l        = this.newL;
                    this.proposed = false;
                    addEpAbstractions(
                            new Ep.EpState(
                                    m.getEpAborted().getEts(),
                                    m.getEpAborted().getValue()
                            )
                    );
                }
                break;

            case EP_DECIDE:
                if (this.ets == m.getEpDecide().getEts() && !this.decided) {
                    this.decided = true;
                    // deliver decision up to app
                    msgQueue.offer(
                            Message.newBuilder()
                                    .setType(Message.Type.UC_DECIDE)
                                    .setSystemId(systemId)
                                    .setFromAbstractionId(id)
                                    .setToAbstractionId("app")
                                    .setUcDecide(
                                            UcDecide.newBuilder()
                                                    .setValue(m.getEpDecide().getValue())
                                                    .build()
                                    )
                                    .build()
                    );
                }
                break;

            default:
                throw new UnsupportedOperationException("uc message not supported: " + m.getType());
        }

        updateLeader(systemId);
    }

    private void updateLeader(String systemId) {
        // if I am the leader and have a defined value & not yet proposed
        if (Utils.getProcessKey(l).equals(Utils.getProcessKey(self))
                && val.getDefined()
                && !proposed) {
            proposed = true;
            msgQueue.offer(
                    Message.newBuilder()
                            .setType(Message.Type.EP_PROPOSE)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id + getEpId())
                            .setEpPropose(
                                    EpPropose.newBuilder()
                                            .setValue(val)
                                            .build()
                            )
                            .build()
            );
        }
    }

    private void addEpAbstractions(Ep.EpState initialState) {
        String epId = getEpId();
        String epRoot = id + epId;
        // Epoch layer
        abstractions.put(epRoot, new Ep(
                id, epRoot, msgQueue, processes, ets, initialState
        ));
        // its BEB and PL layers
        abstractions.put(epRoot + ".beb",
                new BestEffortBroadcast(msgQueue, processes, epRoot + ".beb")
        );
        abstractions.put(epRoot + ".pl",
                pl.createCopyWithParentId(epRoot)
        );
        abstractions.put(epRoot + ".beb.pl",
                pl.createCopyWithParentId(epRoot + ".beb")
        );
    }

    private String getEpId() {
        return ".ep[" + ets + "]";
    }

    @Override
    public void destroy() {
        // nothing to clean up
    }
}
