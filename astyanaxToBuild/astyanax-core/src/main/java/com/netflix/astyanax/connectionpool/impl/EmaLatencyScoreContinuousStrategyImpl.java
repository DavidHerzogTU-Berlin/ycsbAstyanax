package com.netflix.astyanax.connectionpool.impl;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.netflix.astyanax.connectionpool.impl.PendingRequestMap;

/**
 * Calculate latency as an exponential moving average.
 * 
 * @author David H.
 */
public class EmaLatencyScoreContinuousStrategyImpl extends AbstractLatencyScoreStrategyImpl {
    private final static String NAME = "EMAC";
    
    private final double k = .9 ; // cached value for calculation
    private final double one_minus_k; // cached value for calculation
    private long newSample;
    
    public EmaLatencyScoreContinuousStrategyImpl() {
        super(NAME);
        this.one_minus_k = 1 - this.k;
        this.newSample = 0;
    }
    
    @Override
    public final Instance newInstance() {
        return new Instance() {
            private volatile double cachedScore = 0.0d;
            
            @Override
            public String getName() {
            	return NAME;
            }
            
            @Override
            public void addSample(long sample) {
                newSample = sample;
                update();
            }
    
            @Override
            public double getScore() {
                return cachedScore;
            }
    
            @Override
            public void reset() {
                cachedScore = 0.0;
            }
    
            /**
             *  update the cached score
             */
            @Override
            public void update() {
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
