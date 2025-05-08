package com.distributed;

import com.distributed.pl.PerfectLink;

import com.distributed.system.DistributedSystem;
import com.distributed.tcp.Tcp;
import com.distributed.utils.Log;

import java.io.IOException;


import amcds.pb.AmcdsProto.*;
import amcds.pb.AmcdsProto.Message.Type;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) throws Exception {
        Map<String, String> argMap = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            argMap.put(args[i], args[i+1]);
        }
        String owner = argMap.getOrDefault("--owner", "ivan");
        String hubAddress = argMap.getOrDefault("--hub", "127.0.0.1:5000");
        int port = Integer.parseInt(argMap.getOrDefault("--port", "5004"));
        int index = Integer.parseInt(argMap.getOrDefault("--index", "1"));
        String host = "127.0.0.1";

        Log.info("Starting process {}:{}", owner, index);

        BlockingQueue<Message> networkMessages = new LinkedBlockingQueue<>(4096);

        PerfectLink regPl = PerfectLink.create(host, port, hubAddress)
                .createWithProps("", networkMessages, Collections.emptyList());

        String[] hubParts = hubAddress.split(":", 2);
        Message reg = Message.newBuilder()
                .setType(Type.PL_SEND)
                .setPlSend(PlSend.newBuilder()
                        .setDestination(ProcessId.newBuilder()
                                .setHost(hubParts[0])
                                .setPort(Integer.parseInt(hubParts[1]))
                                .build())
                        .setMessage(Message.newBuilder()
                                .setType(Type.PROC_REGISTRATION)
                                .setProcRegistration(ProcRegistration.newBuilder()
                                        .setOwner(owner)
                                        .setIndex(index)
                                        .build())
                                .build())
                        .build())
                .build();

        regPl.handle(reg);
        Log.info("Sent registration to hub at {}", hubAddress);
        Log.info("Registering {}-{} with hub {}", owner, index, hubAddress);
        String listenAddr = host + ":" + port;
        Tcp.listen(listenAddr, data -> {
            try {
                Message m = regPl.parse(data);
                Log.info("Received raw message: {}", m);
                networkMessages.offer(m);
            } catch (IOException e) {
                Log.warn("Failed to parse incoming message: {}", e.getMessage());
            }
        });
        Log.info("{}-{} listening on {}", owner, index, listenAddr);

        Map<String, DistributedSystem> systems = new HashMap<>();

        Thread netThread = new Thread(() -> {
            while (true) {
                try {
                    Message wrapper = networkMessages.take();

                    Message inner = wrapper.getNetworkMessage().getMessage();
                    Type t = inner.getType();

                    switch (t) {
                        case PROC_INITIALIZE_SYSTEM:
                            DistributedSystem sys = DistributedSystem.createSystem(
                                    wrapper, host, owner, hubAddress, port, index
                            );
                            systems.put(wrapper.getSystemId(), sys);
                            sys.startEventLoop();
                            break;

                        case PROC_DESTROY_SYSTEM:
                            DistributedSystem old = systems.remove(wrapper.getSystemId());
                            if (old != null) old.destroy();
                            break;

                        default:
                            DistributedSystem target = systems.get(wrapper.getSystemId());
                            if (target != null) {
                                target.addMessage(wrapper);
                            } else {
                                Log.warn("No system {} to handle {}",
                                        wrapper.getSystemId(), t);
                            }
                            break;
                    }

                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    Log.error("Error in network dispatch: {}", ex.getMessage(), ex);
                }
            }
        }, "Network-Dispatch");
        netThread.setDaemon(true);
        netThread.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("Shutting down...");
            systems.values().forEach(DistributedSystem::destroy);
        }));

        netThread.join();
    }
}
