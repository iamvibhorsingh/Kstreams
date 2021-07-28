package com.learning.kafkastreaming.chapter6;

import java.sql.Timestamp;

public class WebsiteView {

    @Override
    public String toString() {
        return "WebsiteView{" +
                "timestamp=" + timestamp +
                ", user='" + user + '\'' +
                ", topic='" + topic + '\'' +
                ", minutes=" + minutes +
                '}';
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getMinutes() {
        return minutes;
    }


    public void setMinutes(Integer minutes) {
        this.minutes = minutes;
    }

    Timestamp timestamp;
    String user;
    String topic;
    Integer minutes;
}
