package com.nyble.util.consumerActionQ;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.nyble.util.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SequentialConsumerActionQuery implements AdvancedConsumerActionQuery {

    private String joins;
    private List<Column> fields;
    private String fieldsConcat;
    private String consumerActionTableName;
    List<Integer> actions;
    String dsName;

    public SequentialConsumerActionQuery(List<Integer> actions, String joins, List<Column> fields,
                                         String consumerActionTableName, String dsName ){
        this.dsName = dsName;
        this.joins = joins;
        this.fields = fields;
        this.fieldsConcat = fields.stream().map(Column::getName).reduce((s1, s2)->s1+", "+s2).get();
        if(fieldsConcat.endsWith(",")){
            fieldsConcat = fieldsConcat.substring(0, fieldsConcat.length()-1);
        }
        this.consumerActionTableName = consumerActionTableName;
        this.actions = actions;
    }

    @Override
    public void query(String whereClause, Consumer<List<CellObject>> processor) {
        String qTemplate = "SELECT "+fieldsConcat+" from partitionName a "+joins+" where a.id_action in (" +
                "select id from tmp_id_actions) and "+
                whereClause;
        String q = qTemplate.replace("partitionName", consumerActionTableName);
        try(Connection conn = DBUtil.getInstance().getConnection(dsName);
            Statement st = conn.createStatement();
            Statement createTempIdActions = conn.createStatement();
            PreparedStatement insertIntoTmpTbl = conn.prepareStatement("INSERT INTO tmp_id_actions (id) VALUES " +
                    "(?)")){

            createTempIdActions.execute("CREATE TEMP TABLE tmp_id_actions (id INT)");
            for(Integer idAction : actions){
                insertIntoTmpTbl.setInt(1,idAction);
                insertIntoTmpTbl.addBatch();
            }
            insertIntoTmpTbl.executeBatch();

            st.setFetchSize(10000);
            ResultSet rs = st.executeQuery(q);
            while(rs.next()){
                processAction(rs, processor);
            }
            rs.close();
        }catch(Exception e){
            e.printStackTrace();
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

    @Override
    public boolean close() throws Exception {
        return true;
    }
}
