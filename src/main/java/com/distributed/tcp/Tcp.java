package com.distributed.tcp;

import java.io.*;
import java.net.*;
import java.util.function.Consumer;


public class Tcp {

    /**
     * Send a length-prefixed message to the given “host:port” address.
     *
     * @param address "host:port"
     * @param data    raw payload bytes
     * @throws IOException on network errors
     */
    public static void send(String address, byte[] data) throws IOException {
        String[] parts = address.split(":", 2);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            out.writeInt(data.length);
            out.write(data);
        }
    }

    /**
     * Start listening on the given “host:port” and invoke handler for each complete message.
     *
     * @param address "host:port"
     * @param handler callback to handle each payload
     * @return the ServerSocket (so you can close it when done)
     * @throws IOException on bind errors
     */
    public static ServerSocket listen(String address,
                                      Consumer<byte[]> handler) throws IOException {
        String[] parts = address.split(":", 2);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        ServerSocket server = new ServerSocket();
        server.bind(new InetSocketAddress(host, port));

        Thread acceptThread = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    Socket client = server.accept();
                    // handle each connection in its own thread
                    new Thread(() -> handleClient(client, handler), "tcp-handler").start();
                } catch (IOException e) {
                    if (!server.isClosed()) {
                        System.err.println("Error accepting connection: " + e);
                    }
                    break;
                }
            }
        }, "tcp-accept-loop");
        acceptThread.setDaemon(true);
        acceptThread.start();

        return server;
    }

    private static void handleClient(Socket client,
                                     Consumer<byte[]> handler) {
        try (DataInputStream in = new DataInputStream(client.getInputStream())) {
            int len = in.readInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            handler.accept(buf);
        } catch (EOFException eof) {
        } catch (IOException e) {
            System.err.println("TCP read error: " + e);
        } finally {
            try { client.close(); }
            catch (IOException ignored) {}
        }
    }
}
