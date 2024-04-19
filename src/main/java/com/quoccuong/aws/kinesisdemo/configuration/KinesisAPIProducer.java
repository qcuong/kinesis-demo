package com.quoccuong.aws.kinesisdemo.configuration;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.kinesis.common.KinesisClientUtil;

import java.nio.charset.StandardCharsets;

@Slf4j
@Service
public class KinesisAPIProducer {
    final Region REGION_DEFAULT = Region.of("ap-southeast-2");
    final String STREAM_NAME_DEFAULT = "ProcessingOrderStream";
    final KinesisAsyncClient kinesisClient;
    final Gson gson = new Gson();

    public KinesisAPIProducer() {
        kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(REGION_DEFAULT));
    }

    public void putRecord(final String partitionKey, final Object data) {
        var putRecordRequest = PutRecordRequest.builder()
                .data(SdkBytes.fromByteArray(gson.toJson(data).getBytes(StandardCharsets.UTF_8)))
                .streamName(STREAM_NAME_DEFAULT)
                .partitionKey(partitionKey)
                .build();

        kinesisClient.putRecord(putRecordRequest).thenAccept(response -> {
            log.info("Put record successful {}", response.sequenceNumber());
        });
    }
}
