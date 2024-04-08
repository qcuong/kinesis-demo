package com.quoccuong.aws.kinesisdemo.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UserInfo {

    private String userId;

    private String phoneNumber;

}
