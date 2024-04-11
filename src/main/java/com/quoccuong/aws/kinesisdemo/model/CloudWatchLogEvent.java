package com.quoccuong.aws.kinesisdemo.model;

import lombok.Data;

import java.util.List;

@Data
public class CloudWatchLogEvent {

    private String owner;
    private String logGroup;
    private String logStream;
    private List<String> subscriptionFilters;
    private String messageType;
    private List<Event> logEvents;

    @Data
    public static class Event {
        private String id;
        private Long timestamp;
        private String message;
    }

}
