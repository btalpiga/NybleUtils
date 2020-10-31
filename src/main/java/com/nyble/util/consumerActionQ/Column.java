package com.nyble.util.consumerActionQ;

public class Column {
    private int type;
    private String name;
    private boolean isPrimKey;

    private String alias;

    public Column(int type, String name, boolean isPrimaryKey) {
        this.type = type;
        this.name = name;
        this.isPrimKey = isPrimaryKey;
    }

    public Column(int type, String name, boolean isPrimaryKey, String alias) {
        this(type, name, isPrimaryKey);
        this.alias = alias;
    }

    public void setAlias(String a){ alias = a;}

    public int getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public boolean isPrimaryKey(){
        return isPrimKey;
    }

    public String getAlias() {
        return alias;
    }
}
