package com.quoccuong.aws.kinesisdemo.testing;


import com.google.gson.Gson;
import com.quoccuong.aws.kinesisdemo.configuration.KPLProducers;
import com.quoccuong.aws.kinesisdemo.model.UserInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@ConditionalOnProperty(prefix = "kpl.producers", name = "test_producing_message", havingValue = "true")
public class TestingKPLProducer {

    @Autowired
    private KPLProducers kplProducers;

    private Gson gson = new Gson();

    public TestingKPLProducer() {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            var phones = List.of("0945312485", "094356425");
            var userIds = List.of("customer_id_1", "customer_id_2");

            for (int i = 0; i < phones.size(); i++) {
                var userId = userIds.get(i);
                var phone = phones.get(i);
                var json = gson.toJson(
                        UserInfo.builder()
                                .userId(userId)
                                .phoneNumber(phone)
                                .build());
                var data = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
                kplProducers.addUserRecord(data, userId);
            }

        }, 60, 3, TimeUnit.SECONDS);
    }

}
