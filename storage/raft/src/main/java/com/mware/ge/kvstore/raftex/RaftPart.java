package com.mware.ge.kvstore.raftex;

public class RaftPart {
    enum AppendLogResult {
        SUCCEEDED(0),
        E_ATOMIC_OP_FAILURE(-1),
        E_NOT_A_LEADER(-2),
        E_STOPPED(-3),
        E_NOT_READY(-4),
        E_BUFFER_OVERFLOW(-5),
        E_WAL_FAILURE(-6),
        E_TERM_OUT_OF_DATE(-7),
        E_SENDING_SNAPSHOT(-8),
        E_INVALID_PEER(-9),
        E_NOT_ENOUGH_ACKS(-10),
        E_WRITE_BLOCKING(-11);

        int value;

        AppendLogResult(int value) {
            this.value = value;
        }
    }

    enum LogType {
        NORMAL(0x00),
        ATOMIC_OP(0x01),
        /**
         COMMAND is similar to AtomicOp, but not the same. There are two differences:
         1. Normal logs after AtomicOp could be committed together. In opposite, Normal logs
         after COMMAND should be hold until the COMMAND committed, but the logs before
         COMMAND could be committed together.
         2. AtomicOp maybe failed. So we use SinglePromise for it. But COMMAND not, so it could
         share one promise with the normal logs before it.
         * */
        COMMAND(0x02);

        int value;

        LogType(int value) {
            this.value = value;
        }
    }
}
