package consensus;

import amcds.pb.AmcdsProto.*;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;

import java.util.Map;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Eventually Perfect Failure Detector (increasing timeout variant)
 */
public class Epfd implements Abstraction {
    private static final long DELTA = 100;

    private final String id;
    private final String parentId;
    private final BlockingQueue<Message> msgQueue;
    private final List<ProcessId> processes;

    // track alive and suspected processes by key
    private final Map<String, ProcessId> alive = new ConcurrentHashMap<>();
    private final Map<String, ProcessId> suspected = new ConcurrentHashMap<>();

    private long delay;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> timeoutTask;

    public Epfd(String parentAbstraction,
                String abstractionId,
                BlockingQueue<Message> msgQueue,
                List<ProcessId> processes) {
        this.id        = abstractionId;
        this.parentId  = parentAbstraction;
        this.msgQueue  = msgQueue;
        this.processes = processes;
        this.delay     = DELTA;

        // initially all alive
        for (ProcessId p : processes) {
            alive.put(Utils.getProcessKey(p), p);
        }

        startTimer();
    }

    private void startTimer() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
        timeoutTask = scheduler.schedule(this::timeoutEvent, delay, TimeUnit.MILLISECONDS);
    }

    private void timeoutEvent() {
        // enqueue a local timeout event
        msgQueue.offer(
                Message.newBuilder()
                        .setType(Message.Type.EPFD_TIMEOUT)
                        .setFromAbstractionId(id)
                        .setToAbstractionId(id)
                        .setEpfdTimeout(EpfdTimeout.getDefaultInstance())
                        .build()
        );
    }

    @Override
    public void handle(Message m) {
        switch (m.getType()) {
            case EPFD_TIMEOUT:
                handleTimeout(m.getSystemId());
                break;

            case EPFD_INTERNAL_HEARTBEAT_REQUEST:
                // reply immediately
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.PL_SEND)
                                .setSystemId(m.getSystemId())
                                .setFromAbstractionId(id)
                                .setToAbstractionId(id + ".pl")
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setDestination(m.getPlDeliver().getSender())
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                                                                .setSystemId(m.getSystemId())
                                                                .setFromAbstractionId(id)
                                                                .setToAbstractionId(id)
                                                                .setEpfdInternalHeartbeatReply(EpfdInternalHeartbeatReply.getDefaultInstance())
                                                )
                                                .build()
                                )
                                .build()
                );
                break;
            case PL_DELIVER:
                Message inner = m.getPlDeliver().getMessage();
                switch (inner.getType()) {
                    case EPFD_INTERNAL_HEARTBEAT_REQUEST:
                        // reply immediately
                        msgQueue.offer(
                                Message.newBuilder()
                                        .setType(Message.Type.PL_SEND)
                                        .setSystemId(m.getSystemId())
                                        .setFromAbstractionId(id)
                                        .setToAbstractionId(id + ".pl")
                                        .setPlSend(
                                                PlSend.newBuilder()
                                                        .setDestination(m.getPlDeliver().getSender())
                                                        .setMessage(
                                                                Message.newBuilder()
                                                                        .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REPLY)
                                                                        .setSystemId(m.getSystemId())
                                                                        .setFromAbstractionId(id)
                                                                        .setToAbstractionId(id)
                                                                        .setEpfdInternalHeartbeatReply(EpfdInternalHeartbeatReply.getDefaultInstance())
                                                        )
                                                        .build()
                                        )
                                        .build()
                        );
                        break;

                    case EPFD_INTERNAL_HEARTBEAT_REPLY:
                        // mark sender alive
                        ProcessId sender = m.getPlDeliver().getSender();
                        alive.put(Utils.getProcessKey(sender), sender);
                        break;

                    default:
                        throw new UnsupportedOperationException(
                                "epfd pl deliver message type not supported: " + inner.getType()
                        );
                }
                break;

            default:
                throw new UnsupportedOperationException(
                        "epfd message type not supported: " + m.getType()
                );
        }
    }

    private void handleTimeout(String systemId) {
        // if a previously suspected came alive, increase delay
        for (String key : suspected.keySet()) {
            if (alive.containsKey(key)) {
                delay += DELTA;
                break;
            }
        }

        // check all processes
        for (ProcessId p : processes) {
            String key = Utils.getProcessKey(p);
            boolean isAlive     = alive.containsKey(key);
            boolean isSuspected = suspected.containsKey(key);

            if (!isAlive && !isSuspected) {
                // newly suspect
                suspected.put(key, p);
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.EPFD_SUSPECT)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(parentId)
                                .setEpfdSuspect(
                                        EpfdSuspect.newBuilder().setProcess(p)
                                )
                                .build()
                );
            } else if (isAlive && isSuspected) {
                // restore former suspect
                suspected.remove(key);
                msgQueue.offer(
                        Message.newBuilder()
                                .setType(Message.Type.EPFD_RESTORE)
                                .setSystemId(systemId)
                                .setFromAbstractionId(id)
                                .setToAbstractionId(parentId)
                                .setEpfdRestore(
                                        EpfdRestore.newBuilder().setProcess(p)
                                )
                                .build()
                );
            }

            // always send heartbeat request to p
            msgQueue.offer(
                    Message.newBuilder()
                            .setType(Message.Type.EPFD_INTERNAL_HEARTBEAT_REQUEST)
                            .setSystemId(systemId)
                            .setFromAbstractionId(id)
                            .setToAbstractionId(id)
                            .setEpfdInternalHeartbeatRequest(EpfdInternalHeartbeatRequest.getDefaultInstance())
                            .build()
            );
        }

        // clear alive map and restart timer
        alive.clear();
        startTimer();
    }

    @Override
    public void destroy() {
        timeoutTask.cancel(true);
        scheduler.shutdownNow();
    }
}