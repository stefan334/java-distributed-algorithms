package com.distributed.app;

import amcds.pb.AmcdsProto.*;
import amcds.pb.AmcdsProto.Message.*;
import com.distributed.utils.Abstraction;
import com.distributed.utils.Utils;

import java.util.concurrent.BlockingQueue;

public class App implements Abstraction {
    private final BlockingQueue<Message> msgQueue;

    public App(BlockingQueue<Message> msgQueue) {
        this.msgQueue = msgQueue;
    }

    /**
     * Handle a message from any abstraction and emit the next message.
     */
    @Override
    public void handle(Message m) {
        Message msgToSend;

        switch (m.getType()) {
            case PL_DELIVER:
                PlDeliver plDeliver = m.getPlDeliver();
                Message inner = plDeliver.getMessage();
                switch (inner.getType()) {
                    case APP_PROPOSE:
                        msgToSend = Message.newBuilder()
                                .setType(Type.UC_PROPOSE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.uc[" + inner.getAppPropose().getTopic() + "]")
                                .setUcPropose(
                                        UcPropose.newBuilder()
                                                .setValue(inner.getAppPropose().getValue())
                                                .build()
                                )
                                .build();
                        break;
                    case UC_DECIDE:
                        UcDecide dec = m.getUcDecide();
                        Message innerDec = Message.newBuilder()
                                .setType(Type.APP_DECIDE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app")
                                .setAppDecide(
                                        AppDecide.newBuilder()
                                                .setValue(dec.getValue())
                                                .build()
                                )
                                .build();

                        msgToSend = Message.newBuilder()
                                .setType(Type.PL_SEND)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.pl")
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setMessage(innerDec)
                                                .build()
                                )
                                .build();
                        break;
                    case APP_BROADCAST:
                        msgToSend = Message.newBuilder()
                                .setType(Type.BEB_BROADCAST)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.beb")
                                .setBebBroadcast(
                                        BebBroadcast.newBuilder()
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Type.APP_VALUE)
                                                                .setFromAbstractionId("app")
                                                                .setToAbstractionId("app")
                                                                .setAppValue(
                                                                        AppValue.newBuilder()
                                                                                .setValue(inner.getAppBroadcast().getValue())
                                                                                .build()
                                                                )
                                                                .build()
                                                )
                                                .build()
                                )
                                .build();
                        break;
                    case APP_VALUE:
                        msgToSend = Message.newBuilder()
                                .setType(Type.PL_SEND)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.pl")
                                .setPlSend(
                                        PlSend.newBuilder()
                                                .setMessage(
                                                        Message.newBuilder()
                                                                .setType(Type.APP_VALUE)
                                                                .setAppValue(inner.getAppValue())
                                                                .build()
                                                )
                                                .build()
                                )
                                .build();
                        break;
                    case APP_WRITE:
                        msgToSend = Message.newBuilder()
                                .setType(Type.NNAR_WRITE)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.nnar[" + inner.getAppWrite().getRegister() + "]")
                                .setNnarWrite(
                                        NnarWrite.newBuilder()
                                                .setValue(inner.getAppWrite().getValue())
                                                .build()
                                )
                                .build();
                        break;
                    case APP_READ:
                        msgToSend = Message.newBuilder()
                                .setType(Type.NNAR_READ)
                                .setFromAbstractionId("app")
                                .setToAbstractionId("app.nnar[" + inner.getAppRead().getRegister() + "]")
                                .setNnarRead(NnarRead.newBuilder().build())
                                .build();
                        break;
                    default:
                        throw new UnsupportedOperationException("message not supported");
                }
                break;
            case UC_DECIDE:
                UcDecide dec = m.getUcDecide();
                Message innerDec = Message.newBuilder()
                        .setType(Type.APP_DECIDE)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app")
                        .setAppDecide(
                                AppDecide.newBuilder()
                                        .setValue(dec.getValue())
                                        .build()
                        )
                        .build();

                msgToSend = Message.newBuilder()
                        .setType(Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(innerDec)
                                        .build()
                        )
                        .build();
                break;
            case BEB_DELIVER:
                BebDeliver bebDeliver = m.getBebDeliver();
                msgToSend = Message.newBuilder()
                        .setType(Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setType(Type.APP_VALUE)
                                                        .setAppValue(bebDeliver.getMessage().getAppValue())
                                                        .build()
                                        )
                                        .build()
                        )
                        .build();
                break;

            case NNAR_WRITE_RETURN:
                msgToSend = Message.newBuilder()
                        .setType(Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setType(Type.APP_WRITE_RETURN)
                                                        .setAppWriteReturn(
                                                                AppWriteReturn.newBuilder()
                                                                        .setRegister(Utils.getRegisterId(m.getFromAbstractionId()))
                                                                        .build()
                                                        )
                                                        .build()
                                        )
                                        .build()
                        )
                        .build();
                break;

            case NNAR_READ_RETURN:
                msgToSend = Message.newBuilder()
                        .setType(Type.PL_SEND)
                        .setFromAbstractionId("app")
                        .setToAbstractionId("app.pl")
                        .setPlSend(
                                PlSend.newBuilder()
                                        .setMessage(
                                                Message.newBuilder()
                                                        .setType(Type.APP_READ_RETURN)
                                                        .setAppReadReturn(
                                                                AppReadReturn.newBuilder()
                                                                        .setRegister(Utils.getRegisterId(m.getFromAbstractionId()))
                                                                        .setValue(m.getNnarReadReturn().getValue())
                                                                        .build()
                                                        )
                                                        .build()
                                        )
                                        .build()
                        )
                        .build();
                break;

            default:
                throw new UnsupportedOperationException("message not supported");
        }

        msgQueue.offer(msgToSend);
    }

    public void destroy() {
    }
}

