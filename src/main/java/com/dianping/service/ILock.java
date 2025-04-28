package com.dianping.service;

/**
 * @Description: redis 分布式锁实现接口
 * @Author: zhao
 * Created: 2025/4/28 - 20:29
 */
public interface ILock {

    /**
     * 尝试锁定
     * @param timeoutSec 超时秒
     * @return boolean
     */
    boolean tryLock(long timeoutSec);

    /**
     * 开锁
     */
    void unlock();
}
