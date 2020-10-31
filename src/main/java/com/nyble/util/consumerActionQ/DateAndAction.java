package com.nyble.util.consumerActionQ;

import java.sql.Date;

public class DateAndAction{
    private Date from;
    private Date to;
    private int action;

    public DateAndAction(Date from, Date to, int action) {
        this.from = from;
        this.to = to;
        this.action = action;
    }

    public Date getFrom() {
        return from;
    }

    public void setFrom(Date from) {
        this.from = from;
    }

    public Date getTo() {
        return to;
    }

    public void setTo(Date to) {
        this.to = to;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }
}
