package com.dianping.utils;

import lombok.Data;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Accessors(chain = true)
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
