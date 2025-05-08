package com.distributed.system;


import amcds.pb.AmcdsProto.*;



import com.distributed.app.App;
import com.distributed.broadcast.BestEffortBroadcast;
import com.distributed.nnar.NnAtomicRegister;
import com.distributed.pl.PerfectLink;


import com.distributed.utils.Utils;
import com.distributed.utils.Log;
import com.distributed.utils.Abstraction;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Orchestrates all protocol abstractions and drives the event loop.
 */
public class DistributedSystem {
    private final String systemId;
    private final BlockingQueue<Message> msgQueue;
    private final Map<String, Abstraction> abstractions = new HashMap<>();
    private final String hubAddress;
    private final ProcessId ownProcess;
    private final List<ProcessId> processes;
    private volatile boolean running = true;

    private Thread eventLoopThread;

    private DistributedSystem(String systemId,
                              BlockingQueue<Message> msgQueue,
                              ProcessId ownProcess,
                              List<ProcessId> processes,
                              String hubAddress) {
        this.systemId   = systemId;
        this.msgQueue   = msgQueue;
        this.ownProcess = ownProcess;
        this.processes  = processes;
        this.hubAddress = hubAddress;
    }

    /** Factory to create and configure the system from the INIT message. */
    public static DistributedSystem createSystem(
            Message wrapper,
            String host,
            String owner,
            String hubAddress,
            int port,
            int index) {

        ProcInitializeSystem init = wrapper
                       .getNetworkMessage()
                       .getMessage()
                        .getProcInitializeSystem();
        List<ProcessId> procs = init.getProcessesList();

        ProcessId me = procs.stream()
                .filter(p -> p.getOwner().equals(owner) && p.getIndex() == index)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No ProcessId for me"));

        String systemId = wrapper.getSystemId();

        Log.info("{}-{}: Starting system {} ...",
                me.getOwner(), me.getIndex(), systemId);

        DistributedSystem sys = new DistributedSystem(
                systemId,
                new LinkedBlockingQueue<>(4096),
                me,
                procs,
                hubAddress
        );
        sys.registerAbstractions();
        return sys;
    }



    /** Start the background event‐loop thread. */
    public void startEventLoop() {
        eventLoopThread = new Thread(this::runLoop, "System-EventLoop");
        eventLoopThread.start();
    }

    private void runLoop() {
        while (running) {
            String to = "";
            Message m = null;
            try {
                m = msgQueue.take();

                to = m.getToAbstractionId();
                if (!abstractions.containsKey(to) && to.startsWith("app.nnar[")) {
                    String key = Utils.getRegisterId(to);
                    Log.info("Creating new nnar abstraction for {}", to);
                    registerNnarAbstractions(key);
                }

                Abstraction handler = abstractions.get(to);
                if (handler == null) {
                    Log.error("No handler defined for {}", to);
                    continue;
                }

                Log.debug("{} handling message {}", to, m.getType());
                handler.handle(m);

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ex) {
                Log.error("Error in event loop for [{}] handling {}: {}", to, m.getType(), ex.getMessage(), ex);
            }
        }
    }

    /** Register the initial fixed abstractions: app, pl, beb layers. */
    private void registerAbstractions() {
        PerfectLink pl = PerfectLink.create(
                ownProcess.getHost(),
                ownProcess.getPort(),
                hubAddress
        ).createWithProps(
                systemId,
                msgQueue,
                processes
        );

        abstractions.put("app", new App(msgQueue));

        abstractions.put("app.pl", pl.createCopyWithParentId("app"));

        abstractions.put("app.beb",
                new BestEffortBroadcast(msgQueue, processes, "app.beb"));

        abstractions.put("app.beb.pl", pl.createCopyWithParentId("app.beb"));
    }

    /** Dynamically instantiate NNAR register and its layers for a given key. */
    private void registerNnarAbstractions(String key) {
        String aId = "app.nnar[" + key + "]";

        PerfectLink pl = PerfectLink.create(
                ownProcess.getHost(),
                ownProcess.getPort(),
                hubAddress
        ).createWithProps(
                systemId,
                msgQueue,
                processes
        );

        abstractions.put(aId, new NnAtomicRegister(msgQueue, processes.size(), key));

        abstractions.put(aId + ".pl", pl.createCopyWithParentId(aId));

        abstractions.put(aId + ".beb",
                new BestEffortBroadcast(msgQueue, processes, aId + ".beb"));

        abstractions.put(aId + ".beb.pl", pl.createCopyWithParentId(aId + ".beb"));
    }

    public void addMessage(Message m) {
        Log.debug("Received message for %s with type %s", m.getToAbstractionId(), m.getType());
        msgQueue.offer(m);
    }

    /** Shutdown all abstractions and stop the event loop. */
    public void destroy() {
        Log.info("{}-{}: Stopping ...",
                ownProcess.getOwner(),
                ownProcess.getIndex());

        running = false;
        if (eventLoopThread != null) {
            eventLoopThread.interrupt();
        }

        abstractions.values().forEach(Abstraction::destroy);
    }
}
