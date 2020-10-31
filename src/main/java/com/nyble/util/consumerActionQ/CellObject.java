package com.nyble.util.consumerActionQ;

import java.sql.Date;

public class CellObject {

    private Column column;
    private Object data;

    public CellObject(Column column, Object data) {
        this.column = column;
        this.data = data;
    }

    public Date getDate(){
        if(data == null) return null;
        return (Date) data;
    }

    public int getInt(){
        if(data == null) return 0;
        return (int)(Integer)data;
    }

    public double getDouble(){
        if(data == null) return 0;
        return (double)(Double)data;
    }

    public String getString(){
        if(data == null) return null;
        return (String) data;
    }

    public Object getContent() {
        return this.data;
    }

    public String getName (){
        if(this.column.getAlias() != null){
            return this.column.getAlias();
        }else{
            return this.column.getName();
        }
    }

    public int getFieldType(){
        return column.getType();
    }
}
