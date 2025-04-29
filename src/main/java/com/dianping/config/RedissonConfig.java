package com.dianping.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Description: Redisson 配置类
 * @Author: zhao
 * Created: 2025/4/29 - 22:06
 */
@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient() {
        // 配置
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379");
        // 创建RedissonClient对象
        return Redisson.create(config);
    }
}
