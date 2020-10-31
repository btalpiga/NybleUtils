package com.nyble.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class StoreWorkersPool {

    private int poolSize;
    private AtomicInteger currentPoolSize;
    private ConcurrentLinkedQueue<PreparedStatement> statementsPool = new ConcurrentLinkedQueue<>();

    private AtomicBoolean stopWorking = new AtomicBoolean();

    public StoreWorkersPool(int size, String sql, String dsName) throws Exception{
        this.poolSize = size;
        this.currentPoolSize = new AtomicInteger(poolSize);

        for(int i=0; i<poolSize; i++){
            Connection conn = DBUtil.getInstance().getConnection(dsName);
            statementsPool.add(conn.prepareStatement(sql));
        }

    }

    final public PreparedStatement getStatement() throws InterruptedException {
        while(stopWorking.get()){
            Thread.sleep(50);
        }
        PreparedStatement ret ;
        while( (ret = statementsPool.poll()) ==null ){
            Thread.sleep(5);
        }
        currentPoolSize.decrementAndGet();
        return ret;
    }

    final public void releaseStatement(PreparedStatement st){
        currentPoolSize.incrementAndGet();
        statementsPool.add(st);
    }

    private boolean commitLock() throws InterruptedException {
        stopWorking.set(true);
        while(currentPoolSize.get() < poolSize){
            Thread.sleep(50);
        }
        return true;
    }

    private void releaseCommitLock(){
        stopWorking.set(false);
    }

    final public void commit() throws Exception {
        if(this.commitLock()){
            List<Thread> committingThreads = new ArrayList<>();
            for(PreparedStatement ps : statementsPool){
                Thread t = new Thread(()->{
                    try {
                        ps.executeBatch();
                        ps.getConnection().commit();
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                });
                committingThreads.add(t);
                t.start();
            }
            for(Thread t: committingThreads){
                t.join();
            }
            this.releaseCommitLock();
        }
    }

    final public void closeAndCommitPool() throws Exception{
        this.commit();
        for(PreparedStatement ps: statementsPool){
            ps.getConnection().close();
        }
    }

}
