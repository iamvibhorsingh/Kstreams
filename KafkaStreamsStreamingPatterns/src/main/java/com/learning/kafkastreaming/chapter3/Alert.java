package com.learning.kafkastreaming.chapter3;

import java.sql.Timestamp;

public class Alert {

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMesg() {
        return mesg;
    }

    public void setMesg(String mesg) {
        this.mesg = mesg;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "timestamp=" + timestamp +
                ", level='" + level + '\'' +
                ", code='" + code + '\'' +
                ", mesg='" + mesg + '\'' +
                '}';
    }

    Timestamp timestamp;
    String level;
    String code;
    String mesg;

}
