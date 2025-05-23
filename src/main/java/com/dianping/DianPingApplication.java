package com.dianping;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @Description: 启动类
 * @Author: zhao
 * Created: 2025/4/14 - 17:05
 */
@EnableAspectJAutoProxy(exposeProxy = true)
@MapperScan("com.dianping.mapper")
@SpringBootApplication
public class DianPingApplication {
    public static void main(String[] args) {
        SpringApplication.run(DianPingApplication.class, args);
    }
}
