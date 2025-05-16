package consensus;


import amcds.pb.AmcdsProto.*;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Eld implements Abstraction {
    private final String id;
    private final String parentId;
    private final BlockingQueue<Message> msgQueue;
    private final List<ProcessId> processes;

    // map from owner-index key to ProcessId
    private final Map<String, ProcessId> alive = new LinkedHashMap<>();
    private ProcessId leader = null;

    public Eld(String parentAbstraction,
               String abstractionId,
               BlockingQueue<Message> msgQueue,
               List<ProcessId> processes) {
        this.id        = abstractionId;
        this.parentId  = parentAbstraction;
        this.msgQueue  = msgQueue;
        this.processes = processes;

        // initialize all as alive
        for (ProcessId p : processes) {
            alive.put(Utils.getProcessKey(p), p);
        }
    }

    @Override
    public void handle(Message m) {
        String systemId = m.getSystemId();
        switch (m.getType()) {
            case EPFD_SUSPECT:
                String suspectKey = Utils.getProcessKey(m.getEpfdSuspect().getProcess());
                alive.remove(suspectKey);
                break;

            case EPFD_RESTORE:
                ProcessId restored = m.getEpfdRestore().getProcess();
                alive.put(Utils.getProcessKey(restored), restored);
                break;

            default:
                throw new UnsupportedOperationException("eld unknown message type: " + m.getType());
        }

        // after updating alive set, recompute leader
        updateLeader(systemId);
    }

    private void updateLeader(String systemId) {
        // pick the alive process with max rank
        ProcessId max = alive.values().stream()
                .max(Utils.byRank())
                .orElse(null);

        if (max == null) {
            throw new IllegalStateException("could not find process with max rank when electing leader");
        }

        // if changed, emit ELD_TRUST to parent
        if (leader == null || !Utils.getProcessKey(leader).equals(Utils.getProcessKey(max))) {
            leader = max;
            msgQueue.offer(
                    Message.newBuilder()
                            .setType(Message.Type.ELD_TRUST)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(parentId)
                            .setEldTrust(
                                    EldTrust.newBuilder()
                                            .setProcess(max)
                                            .build()
                            )
                            .build()
            );
        }
    }

    @Override
    public void destroy() {
        // nothing to clean up
    }
}
