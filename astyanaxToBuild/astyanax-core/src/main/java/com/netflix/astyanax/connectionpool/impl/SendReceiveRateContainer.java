package com.netflix.astyanax.connectionpool.impl;

import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class SendReceiveRateContainer {
        // Constants for send/receive rate tracking
        private static final long RATE_INTERVAL_MS = 20;
        private static final long RECEIVE_RATE_INITIAL = 100;

        // Constants for cubic function
        private static final double CUBIC_BETA = 0.2;
        private static final double CUBIC_C = 0.000004;
        private static final double CUBIC_SMAX = 10;
        private static final double CUBIC_HYSTERISIS_FACTOR = 4;
        private static final double CUBIC_BETA_BY_C = CUBIC_BETA / CUBIC_C;
        private static final double CUBIC_HYSTERISIS_DURATION = RATE_INTERVAL_MS * CUBIC_HYSTERISIS_FACTOR;

        private final InetAddress endpoint;
        private final SimpleRateLimiter sendingRateLimiter;
        private final SlottedRateTracker receiveRateTracker;

        // Cubic growth variables
        private long timeOfLastRateDecrease = 0L;
        private long timeOfLastRateIncrease = 0L;
        private double Rmax = 0;

        // Cubic score for replica selection, updated on a per-request level
        private static final double SCORE_EMA_ALPHA = 0.9;
        private static final double ONE_MINUS_SCORE_EMA_ALPHA = 1 - SCORE_EMA_ALPHA;
        private double emaQSZ = 0;
        private double emaMu = 0;
        private double emaNw = 0;

        SendReceiveRateContainer(InetAddress endpoint) {
            this.endpoint = endpoint;
            this.sendingRateLimiter = new SimpleRateLimiter(1, RATE_INTERVAL_MS, 50);
            this.receiveRateTracker = new SlottedRateTracker(RECEIVE_RATE_INITIAL, RATE_INTERVAL_MS);
        }

        public double tryAcquire() {
            return sendingRateLimiter.tryAcquire();
        }

        public void receiveRateTrackerTick() {
            receiveRateTracker.add(1);
        }

        public double getReceiveRate() {
            return receiveRateTracker.getCurrentRate();
        }
        public synchronized void updateNodeScore(int feedbackQSZ,
                                                 double feedbackMu,
                                                 double feedbackResponseTime) {
            emaQSZ = SCORE_EMA_ALPHA * feedbackQSZ  + ONE_MINUS_SCORE_EMA_ALPHA * emaQSZ;
            emaMu = SCORE_EMA_ALPHA * feedbackMu  + ONE_MINUS_SCORE_EMA_ALPHA * emaMu;
            final double nwRtt = (feedbackResponseTime - feedbackMu);
            emaNw = SCORE_EMA_ALPHA * nwRtt  + ONE_MINUS_SCORE_EMA_ALPHA * emaNw;
            assert (feedbackMu < feedbackResponseTime);
        }
        
        public synchronized double getScore() {
            AtomicInteger counter = PendingRequestMap.getPendingRequestsAtomic(endpoint.getHostAddress());
            if (counter == null) {
                return 0.0;
            }
            return emaNw + Math.pow(1 + emaQSZ + (PendingRequestMap.getMap_size()* counter.get()), 3) * emaMu;
        }

        public synchronized void updateCubicSendingRate() {
            final double currentReceiveRate = receiveRateTracker.getCurrentRate();
            final double currentSendingRate = sendingRateLimiter.getRate();
            final long now = System.currentTimeMillis();

            if (currentSendingRate > currentReceiveRate
                    && (now - timeOfLastRateIncrease > CUBIC_HYSTERISIS_DURATION)) {
                Rmax = currentSendingRate;
                sendingRateLimiter.setRate(Math.max(currentSendingRate * CUBIC_BETA, 0.001));
                timeOfLastRateDecrease = now;
            }
            else if (currentSendingRate < currentReceiveRate) {
                final double T = System.currentTimeMillis() - timeOfLastRateDecrease;
                timeOfLastRateIncrease = now;
                final double scalingFactor = Math.cbrt(Rmax * CUBIC_BETA_BY_C);
                final double newSendingRate = CUBIC_C * Math.pow(T - scalingFactor, 3) + Rmax;

                if (newSendingRate - currentSendingRate > CUBIC_SMAX) {
                    sendingRateLimiter.setRate(currentSendingRate + CUBIC_SMAX);
                } else {
                    sendingRateLimiter.setRate(newSendingRate);
                }

                assert(newSendingRate > 0);
            }
        }
    }