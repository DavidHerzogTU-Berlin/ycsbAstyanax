package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;

/**
 * Calculate latency as an exponential moving average.
 * 
 * @author David H.
 */
public class EmaLatencyScoreContinuousStrategyImpl extends AbstractLatencyScoreStrategyImpl {
    private final static String NAME = "EMA";
    
    private static final double N = 100;
    private final double k; // cached value for calculation
    private final double one_minus_k; // cached value for calculation
    private long newSample;
    
    public EmaLatencyScoreContinuousStrategyImpl() {
        super(NAME);
        
        this.k = (double)2 / (double)(this.N + 1);
        this.one_minus_k = 1 - this.k;
        this.newSample = 0;
        System.out.println("EmaLatencyScoreContinuousStrategyImpl()2/1.9");
    }
    
    @Override
    public final Instance newInstance() {
        return new Instance() {
            private volatile double cachedScore = 0.0d;
    
            @Override
            public void addSample(long sample) {
                newSample = sample;
                update();
                
            }
    
            @Override
            public double getScore() {
                System.out.println("EmaLatencyScoreContinuousStrategyImpl.getScore(): " + cachedScore);
                return cachedScore;
            }
    
            @Override
            public void reset() {
                cachedScore = 0.0;
            }
    
            /**
             * Drain all the samples and update the cached score
             */
            @Override
            public void update() {
                System.out.println("EmaLatencyScoreContinuousStrategyImpl.update()");
                Double ema = cachedScore;
                if(newSample == 0) {
                    cachedScore = 0.0d;
                } else {
                    ema = newSample * k + ema * one_minus_k;
                    cachedScore = ema;
                }
                
                
                
            }
        };
    }
}
