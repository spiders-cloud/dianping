package com.dianping.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.dianping.service.ILock;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: zhao
 * Created: 2025/4/28 - 20:30
 */
public class SimpleRedisLock implements ILock {

    private final String lockName;
    private final StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String lockName, StringRedisTemplate stringRedisTemplate) {
        this.lockName = lockName;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";

    @Override
    public boolean tryLock(long timeoutSec) {
        // 获取线程标识
        String threadId = String.valueOf(Thread.currentThread().getId());
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                                             .setIfAbsent(KEY_PREFIX + lockName, threadId, timeoutSec,
                                                     TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    @Override
    public void unlock() {
        // 通过del删除锁
        stringRedisTemplate.delete(KEY_PREFIX + lockName);
    }
}
