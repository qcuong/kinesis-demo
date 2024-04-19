package com.quoccuong.aws.kinesisdemo.configuration;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quoccuong.aws.kinesisdemo.model.CloudWatchLogEvent;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

import java.util.UUID;
import java.util.zip.GZIPInputStream;

@Slf4j
public class KCLConsumers {
    final String REGION_DEFAULT = "ap-southeast-2";
    final String STREAM_NAME_DEFAULT = "LogAggregationKDS";
    final String WORKER_IDENTIFIER = UUID.randomUUID().toString();

    final String APPLICATION_NAME = "kinesis-demo-app";

    public KCLConsumers() {
        var region = Region.of(REGION_DEFAULT);

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(region));

        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();

        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                STREAM_NAME_DEFAULT,
                APPLICATION_NAME,
                kinesisClient,
                dynamoClient,
                cloudWatchClient,
                WORKER_IDENTIFIER,
                new SampleRecordProcessorFactory());

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    private static class SampleRecordProcessorFactory implements ShardRecordProcessorFactory {
        public ShardRecordProcessor shardRecordProcessor() {
            return new SampleRecordProcessor();
        }
    }

    private static class SampleRecordProcessor implements ShardRecordProcessor {
        private static final String SHARD_ID_MDC_KEY = "ShardId";
        private String shardId;

        final ObjectMapper objectMapper = new ObjectMapper();

        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void processRecords(ProcessRecordsInput processRecordsInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            log.info("Processing {} record(s)", processRecordsInput.records().size());
            processRecordsInput.records().forEach(record -> {
                var data = "";
                try {
                    byte[] arr = new byte[record.data().remaining()];
                    record.data().get(arr);
//                        data = new String(arr);
                    data = decompressGzipFile(arr);
                    var logEvent = objectMapper.readValue(data, CloudWatchLogEvent.class);

                    log.info("Processing record pk: {} -- Seq: {} -- SubSeq: {}",
                            record.partitionKey(),
                            record.sequenceNumber(),
                            record.subSequenceNumber());
                    log.info("Processing record logEvent: {}",
                            logEvent);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            MDC.remove(SHARD_ID_MDC_KEY);
        }

        public void leaseLost(LeaseLostInput leaseLostInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Lost lease, so terminating.");
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void shardEnded(ShardEndedInput shardEndedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Reached shard end checkpointing.");
                shardEndedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at shard end. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            MDC.put(SHARD_ID_MDC_KEY, shardId);
            try {
                log.info("Scheduler is shutting down, checkpointing.");
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
            } finally {
                MDC.remove(SHARD_ID_MDC_KEY);
            }
        }

        private String decompressGzipFile(byte[] data) {
            var response = "";
            ByteArrayOutputStream fos = null;
            GZIPInputStream gis = null;
            try {
                gis = new GZIPInputStream(new ByteArrayInputStream(data));
                fos = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gis.read(buffer)) != -1) {
                    fos.write(buffer, 0, len);
                }
                response = new String(fos.toByteArray());
                fos.close();
                gis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return response;
        }
    }
}
