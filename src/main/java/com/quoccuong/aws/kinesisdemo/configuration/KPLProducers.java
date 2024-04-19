package com.quoccuong.aws.kinesisdemo.configuration;

import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class KPLProducers {
    final String REGION_DEFAULT = "ap-southeast-2";
    final String STREAM_NAME_DEFAULT = "ProcessingOrderStream";
    final KinesisProducer kinesisProducer;

    public KPLProducers() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(10000)
                .setMaxConnections(1)
                .setRequestTimeout(60000)
                .setRegion(REGION_DEFAULT);
        kinesisProducer = new KinesisProducer(config);
    }

    public void addUserRecord(ByteBuffer data, String partitionKey) {
        ListenableFuture<UserRecordResult> f = kinesisProducer
                .addUserRecord(STREAM_NAME_DEFAULT, partitionKey, data);

        Futures.addCallback(f, callback, Executors.newSingleThreadExecutor());
    }

    final FutureCallback<UserRecordResult> callback = new FutureCallback<>() {
        @Override
        public void onFailure(Throwable t) {
            if (t instanceof UserRecordFailedException) {
                Attempt last = Iterables.getLast(
                        ((UserRecordFailedException) t).getResult().getAttempts());
                log.error(String.format(
                        "Record failed to put - %s : %s",
                        last.getErrorCode(), last.getErrorMessage()));
            }
            log.error("Exception during put", t);
        }

        @Override
        public void onSuccess(UserRecordResult result) {
            log.error(String.format(
                "Record successfully to put - %s : %s",
                    result.getSequenceNumber(), result.getShardId()));
        }
    };
}
