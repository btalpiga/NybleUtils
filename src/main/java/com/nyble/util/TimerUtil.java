package com.nyble.util;

import java.util.concurrent.atomic.AtomicLong;

public class TimerUtil {
    private long startTime;
    private long endTime;
    private long delta;

    private String name;
    public TimerUtil (String n){
        this.name = n;
    }

    public long start(){
        startTime = System.currentTimeMillis();
        return startTime;
    }

    public long stop(){
        endTime = System.currentTimeMillis();
        delta = endTime-startTime;
        return endTime;
    }

    public void reset(){
        endTime = 0;
        startTime = 0;
        delta = 0;
    }

    public long getSecondsSpent(){
        return (delta< 0 ? -1: ( Math.round ((double)(delta * 100)/1000)));
    }

    public long getMillisSpent(){
        return (delta<0 ? -1 : delta);
    }

    public long accumulateTimer(AtomicLong acc){
        return acc.accumulateAndGet(this.delta, Long::sum);
    }

    public String toString(){
        return "Timer "+name+" took "+delta+" ms";
    }

}
