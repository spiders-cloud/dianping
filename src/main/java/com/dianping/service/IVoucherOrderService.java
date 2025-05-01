package com.dianping.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.dianping.dto.Result;
import com.dianping.entity.VoucherOrder;

/**
 * <p>
 * 服务类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result createVoucherOrderV1(Long voucherId);

    void createVoucherOrderV2(VoucherOrder voucherOrder);
}
