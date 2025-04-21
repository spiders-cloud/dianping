package com.dianping.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dianping.dto.LoginFormDTO;
import com.dianping.dto.Result;
import com.dianping.dto.UserDTO;
import com.dianping.entity.User;
import com.dianping.mapper.UserMapper;
import com.dianping.service.IUserService;
import com.dianping.utils.RegexUtils;
import jakarta.annotation.Resource;
import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dianping.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送验证码
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        // 3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4.保存验证码到 redis中
        // 4.1 设置key
        String key = LOGIN_CODE_KEY + phone;
        // 4.2 设置验证码的有效期
        stringRedisTemplate.opsForValue()
                           .set(key, code, LOGIN_CODE_TTL, java.util.concurrent.TimeUnit.MINUTES);
        // 5.发送验证码
        log.debug("发送短信验证码成功，验证码：{}", code);
        // 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误！");
        }
        // 2.从redis中获取验证码并校验
        String cacheCode = stringRedisTemplate.opsForValue()
                                              .get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            // 3.不一致，报错
            return Result.fail("验证码错误");
        }
        // 4.一致，根据手机号查询用户 select * from tb_user where phone = ?
        User user = query().eq("phone", phone)
                           .one();

        // 5.判断用户是否存在
        if (user == null) {
            // 6.不存在，则创建
            user = createUserWithPhone(phone);
        }
        // 7.保存用户信息到redis中
        // 7.1随机生成token，作为登录令牌
        String token = UUID.randomUUID()
                           .toString(true);
        // 7.2将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(
                userDTO, new HashMap<>(), CopyOptions.create()
                                                     .setIgnoreNullValue(true)
                                                     .setFieldValueEditor(
                                                             (fieldName, fieldValue) -> fieldValue.toString()));
        // 7.3存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash()
                           .putAll(tokenKey, userMap);
        // 7.4设置token有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User().setPhone(phone)
                              .setNickName(phone);
        save(user);
        return user;
    }
}
