package com.learning.kafkastreaming.chapter6;

public class ViewAggregator {

    private Integer totalMinutes = 0;

    public Integer getTotalValue() {
        return totalMinutes;
    }

    public ViewAggregator add(Integer value) {

        totalMinutes += value;
        return this;
    }

}
