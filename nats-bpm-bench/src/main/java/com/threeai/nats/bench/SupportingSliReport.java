package com.threeai.nats.bench;

/** Supporting SLIs — SYS_BENCH_SLI_DRIFT (warn-only, NOT a hard gate). */
public record SupportingSliReport(
        double dispatchLatencyP95Ms, double lockWaitMsAvg, int hikariActiveConnectionsAvg) {

    public boolean withinSoftTargets() {
        return dispatchLatencyP95Ms <= 200.0; // NFR-P2 — reported only, never fails the build
    }
}
