package com.dianping.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dianping.dto.Result;
import com.dianping.entity.SeckillVoucher;
import com.dianping.entity.VoucherOrder;
import com.dianping.mapper.VoucherOrderMapper;
import com.dianping.service.ISeckillVoucherService;
import com.dianping.service.IVoucherOrderService;
import com.dianping.utils.RedisIdWorker;
import com.dianping.utils.UserHolder;
import jakarta.annotation.Resource;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    /**
     * Seckiill 优惠券
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        // return subStockV3(voucherId);
        // return subStockV4(voucherId);
        return subStockV5(voucherId);
    }

    /**
     * 扣减库存V5
     * 利用Redis 分布式锁解决超卖问题 - V1
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    private Result subStockV5(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 获取锁对象
        boolean isLock = lock.tryLock(1200);
        // 加锁失败
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            // 获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 扣减库存V4 - Redisson
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    private Result subStockV4(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("order:" + userId);
        // 获取锁对象
        boolean isLock = lock.tryLock();
        // 加锁失败
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            // 获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 扣减库存：V1、V2、V3
     * 利用synchronized 锁解决超卖问题
     * 利用AopContext.currentProxy()获取代理对象（事务）
     * 利用intern()方法将字符串存入常量池，避免重复创建字符串对象
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     * @problem 无法解决分布式锁问题
     */
    private Result subStockV3(Long voucherId) {
        // 5.扣减库存
        // 5.1 V1 直接扣减库存
        // boolean success = seckillVoucherService
        //         .update()
        //         .setSql("stock= stock -1")
        //         .eq("voucher_id", voucherId)
        //         .update();

        // 5.2 V2 利用版本号-库存容量判断库存是否充足（乐观锁）
        // boolean success = seckillVoucherService
        //         .update()
        //         .setSql("stock= stock -1")
        //         .eq("voucher_id", voucherId)
        //         .eq("stock", voucher.getStock())
        //         .update();
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
    }

    @Override
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 5.1.查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2.判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            return Result.fail("用户已经购买过一次！");
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1") // set stock = stock - 1
                                               .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                                               .update();
        if (!success) {
            // 扣减失败
            return Result.fail("库存不足！");
        }

        // 7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 7.1.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 7.2.用户id
        voucherOrder.setUserId(userId);
        // 7.3.代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        // 7.返回订单id
        return Result.ok(orderId);
    }
}
