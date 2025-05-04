package com.dianping.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dianping.dto.Result;
import com.dianping.entity.SeckillVoucher;
import com.dianping.entity.VoucherOrder;
import com.dianping.mapper.VoucherOrderMapper;
import com.dianping.service.ISeckillVoucherService;
import com.dianping.service.IVoucherOrderService;
import com.dianping.utils.RedisIdWorker;
import com.dianping.utils.UserHolder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
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

    // ==============================================MQ方式============================================== //

    /**
     * 秒杀优惠券V3 - Stream MQ
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    @Override
    public Result seckillVoucherV3(Long voucherId) {
        // 获取用户·
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString()
        );
        // 判断购买资格
        if (result != 0) {
            // 不为0代表没有资格购买
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    /**
     * 凭证订购任务V2 - Stream MQ
     * @author zhao
     * @date 2025/05/01
     */
    private class VoucherOrderTaskV2 implements Runnable {
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    handleVoucherOrderV2(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 处理异常消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list =
                            stringRedisTemplate.opsForStream().read(
                                    Consumer.from("g1", "c1"),
                                    StreamReadOptions.empty().count(1),
                                    StreamOffset.create("stream.orders", ReadOffset.from("0"))
                            );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    handleVoucherOrderV2(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }


    /**
     * 处理优惠券订单V2 - Stream MQ
     * @param voucherOrder 优惠券订购
     */
    private void handleVoucherOrderV2(VoucherOrder voucherOrder) {
        // 获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("order:" + userId);
        // 获取锁对象
        boolean isLock = lock.tryLock();
        // 加锁失败
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            // 注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy.createVoucherOrderV2(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }


    // ==============================================异步阻塞队列============================================ //

    /**
     * 秒杀优惠券V2 - 异步方式
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    @Override
    public Result seckillVoucherV2(Long voucherId) {
        // 获取用户·
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString()
        );
        // 判断购买资格
        if (result != 0) {
            // 不为0代表没有资格购买
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
        log.info("用户{}有购买资格", userId);
        // 为0，则有购买资格
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(redisIdWorker.nextId("order")).setUserId(userId).setVoucherId(voucherId);
        // 放入阻塞队列
        orderTasks.add(voucherOrder);
        log.info("用户{}订单放入阻塞队列", userId);
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    // 秒杀Lua脚本-判断用户是否有购买资格
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private IVoucherOrderService proxy;

    // 异步处理线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderTaskV2()); // VoucherOrderTaskV1
    }

    /**
     * 凭证订购任务V1 - 阻塞队列
     * @author zhao
     * @date 2025/05/01
     */
    private class VoucherOrderTaskV1 implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrderV1(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    /**
     * 处理优惠券订单V1 - 阻塞队列
     * @param voucherOrder 优惠券订购
     */
    private void handleVoucherOrderV1(VoucherOrder voucherOrder) {
        // 获取用户
        Long userId = voucherOrder.getUserId();
        // 创建锁对象
        RLock lock = redissonClient.getLock("order:" + userId);
        // 获取锁对象
        boolean isLock = lock.tryLock();
        // 加锁失败
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            // 注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy.createVoucherOrderV2(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 创建优惠券订单V2 - 异步
     * @param voucherOrder 优惠券订购
     */
    @Transactional
    @Override
    public void createVoucherOrderV2(VoucherOrder voucherOrder) {
        // 这些判断有些冗余，因为redis中已经判断该用户有购买资格，直接返回给前端了
        // 然后后台的购买任务放入阻塞队列中排队，执行该任务就一定可行，不存在库存不足、一人一单问题
        // 兜底操作
        Long userId = voucherOrder.getUserId();
        // 5.1.查询订单
        int count = Math.toIntExact(query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId())
                                           .count());
        // 5.2.判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过了");
            return;
        }

        // 6.扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1").eq(
                "voucher_id",
                voucherOrder.getVoucherId()
        ).gt("stock", 0).update();
        log.info("扣减库存成功");
        if (!success) {
            // 扣减失败
            log.error("库存不足");
            return;
        }
        save(voucherOrder);

    }


    // ==============================================串行执行============================================== //

    /**
     * 秒杀优惠券V1 - 串行方式
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    @Override
    public Result seckillVoucherV1(Long voucherId) {
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
     * 扣减库存V5- Redisson
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    private Result subStockV5(Long voucherId) {
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
            return proxy.createVoucherOrderV1(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }


    /**
     * 扣减库存V4
     * 利用Redis 分布式锁解决超卖问题
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    private Result subStockV4(Long voucherId) {
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
            return proxy.createVoucherOrderV1(voucherId);
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
            return proxy.createVoucherOrderV1(voucherId);
        }
    }


    /**
     * 创建优惠券订单-V1
     * @param voucherId 优惠券 ID
     * @return {@link Result }
     */
    @Override
    @Transactional
    public Result createVoucherOrderV1(Long voucherId) {
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
