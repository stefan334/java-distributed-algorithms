package com.distributed.utils;

import amcds.pb.AmcdsProto.*;





public interface Abstraction {
    /**
     * Handle an incoming Message.
     * @param m the protobuf Message
     */
    void handle(Message m) throws Exception;

    void destroy();
}
