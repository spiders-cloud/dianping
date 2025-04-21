package com.dianping.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dianping.entity.ShopType;
import com.dianping.mapper.ShopTypeMapper;
import com.dianping.service.IShopTypeService;
import com.dianping.utils.RedisConstants;
import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 查询店铺类型列表
     * @return
     */
    @Override
    public List<ShopType> queryTypeList() {
        String key = RedisConstants.CACHE_SHOP_TYPE_KEY;
        // 1. 查询redis
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);
        // 2. 判断是否存在
        if (StrUtil.isNotBlank(shopTypeJson)) {
            // 3. 存在，直接返回
            return JSONUtil.toList(shopTypeJson, ShopType.class);
        }
        // 4. 不存在，根据id查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        // 5. 不存在，返回错误
        if (shopTypeList == null) {
            return null;
        }
        // 6. 存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList));
        return shopTypeList;
    }
}
