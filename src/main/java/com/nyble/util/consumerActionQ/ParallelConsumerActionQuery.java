package com.nyble.util.consumerActionQ;

import com.nyble.util.DBUtil;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class ParallelConsumerActionQuery implements AdvancedConsumerActionQuery{

    private Map<Integer, Set<String>> actionsToPartitions = new HashMap<>();
    private Map<GregorianCalendar, Set<String>> datesToPartitions = new HashMap<>();
    private List<Column> fields;
    private String joins;
    private String fieldsConcat;
    private ExecutorService ex;
    private Set<String> partitionsInvolved = new HashSet<>();
    private  Connection[] connectionPool;
    private String dsName;

    public ParallelConsumerActionQuery(List<DateAndAction> actions, String joins,
                                       List<Column> fields, int poolSize, String dsName) throws SQLException {
        this.joins = joins;
        this.fields = fields;
        this.dsName = dsName;

        ex = Executors.newFixedThreadPool(poolSize);
        fieldsConcat = fields.stream().map(Column::getName).reduce((s1, s2)->s1+", "+s2).get();
        if(fieldsConcat.endsWith(",")){
            fieldsConcat = fieldsConcat.substring(0, fieldsConcat.length()-1);
        }

        connectionPool = new Connection[poolSize];
        for (int i=0;i<connectionPool.length; i++){
            connectionPool[i] = DBUtil.getInstance().getConnection(dsName);
        }

        initPartitionMappers();
        for(DateAndAction d : actions){
            partitionsInvolved.addAll(getPartitionNameOffline(d));
        }
    }

    private void initPartitionMappers(){
        try(Connection conn = DBUtil.getInstance().getConnection(dsName);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT partition_name, start_date, id_action " +
                    "from "+TABLE_CONSUMER_ACTION_MAPPER)){

            while(rs.next()){
                String partName = rs.getString(1);
                Date d = rs.getDate(2);
                int idAction = rs.getInt(3);
                GregorianCalendar startDate = new GregorianCalendar();
                startDate.setTime(new java.util.Date(d.getTime()));
                startDate.set(Calendar.HOUR_OF_DAY, 0);
                startDate.set(Calendar.MINUTE, 0);
                startDate.set(Calendar.SECOND,0);
                startDate.set(Calendar.MILLISECOND,0);
                startDate.set(Calendar.MONTH, 1);
                startDate.set(Calendar.DAY_OF_MONTH,1);

                actionsToPartitions.compute(idAction, (key, val)->{
                    if(val == null){
                        val = new HashSet<>();
                    }
                    val.add(partName);
                    return val;
                });

                datesToPartitions.compute(startDate, (key, val)->{
                    if(val == null){
                        val = new HashSet<>();
                    }
                    val.add(partName);
                    return val;
                });
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public ParallelConsumerActionQuery(List<DateAndAction> actions, String joins, List<Column> fields, String dsName) throws Exception {
        this(actions,joins,fields,80, dsName );
    }

    private ArrayList<String> getPartitionNameOffline(DateAndAction act) {
        GregorianCalendar start = new GregorianCalendar();
        start.setTime(new java.util.Date(act.getFrom().getTime()));
        start.set(Calendar.HOUR_OF_DAY, 0);
        start.set(Calendar.MINUTE, 0);
        start.set(Calendar.SECOND,0);
        start.set(Calendar.MILLISECOND,0);
        start.set(Calendar.MONTH, 1);
        start.set(Calendar.DAY_OF_MONTH,1);

        GregorianCalendar end = new GregorianCalendar();
        end.setTime(new java.util.Date(act.getTo().getTime()));
        end.set(Calendar.HOUR_OF_DAY, 0);
        end.set(Calendar.MINUTE, 0);
        end.set(Calendar.SECOND,0);
        end.set(Calendar.MILLISECOND,0);
        end.set(Calendar.MONTH, 1);
        end.set(Calendar.DAY_OF_MONTH,1);

        Set<String> partitionsByAct = actionsToPartitions.get(act.getAction());
        if(partitionsByAct == null){
            partitionsByAct = new HashSet<>();
        }
        Set<String> partitionsByDate = new HashSet<>();
        while(start.before(end)){
            if(datesToPartitions.get(start) != null){
                partitionsByDate.addAll(datesToPartitions.get(start));
            }
            start.add(Calendar.YEAR,1);
        }

        start.setTime(new java.util.Date(act.getTo().getTime()));
        if(end.before(start)){
            partitionsByDate.addAll(datesToPartitions.get(end));
        }

        ArrayList<String> partitionNames = new ArrayList<>();
        for(String s: partitionsByAct){
            if(partitionsByDate.contains(s)){
                partitionNames.add(s);
            }
        }
        return partitionNames;
    }

    public void query (String whereClause, Consumer<List<CellObject>> processor) {

        String qTemplate = "SELECT "+fieldsConcat+" from partitionName a "+joins+" where "+whereClause;
        int connIndex = 0;
        for(String partition: partitionsInvolved){
            String q = qTemplate.replace("partitionName", partition);
            //run each in parallel
            //get result into a concurrent map
            final int idx = connIndex%connectionPool.length;
            ex.submit(()-> {
                try {
                    runQuery(q, connectionPool[idx], processor, partition);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            connIndex++;
        }
    }

    public void runQuery(String q,Connection conn, Consumer<List<CellObject>> processor, String partitionName) throws Exception{
        try(Statement st = conn.createStatement()){
            st.setFetchSize(10000);
            ResultSet rs = st.executeQuery(q);
            while(rs.next()){
                processAction(rs, processor);
            }
            rs.close();
            System.out.println(">>>>>>>>>"+partitionName+" finished");
        }
    }

    private void processAction(ResultSet rs, Consumer<List<CellObject>> processRow) throws Exception{

        List<CellObject> row = new ArrayList<>();
        for(int i=0;i<fields.size();i++){
            CellObject cellData;
            if(fields.get(i).getName().toUpperCase().endsWith("VALUE")){
                cellData = new CellObject(fields.get(i), rs.getString(i+1));
            }else{
                cellData = new CellObject(fields.get(i), rs.getObject(i+1));
            }
            row.add(cellData);
        }
        processRow.accept(row);

    }

    public boolean close() throws Exception{
        ex.shutdown();
        while(!ex.isTerminated()){
            Thread.sleep(100);
        }
        for (Connection connection : connectionPool) {
            connection.commit();
            connection.close();
        }
        return true;
    }
}

