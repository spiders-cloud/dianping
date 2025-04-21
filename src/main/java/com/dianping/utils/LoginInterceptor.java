package com.dianping.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.dianping.dto.UserDTO;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Description: 登录拦截器
 * @Author: zhao
 * Created: 2025/4/20 - 19:29
 */
@Slf4j
public class LoginInterceptor implements HandlerInterceptor {
    // 不能使用注解注入，因为拦截器是在Spring容器之外创建的，所以需要手动注入
    private final StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        // 1.获取请求头中的token
        String token = request.getHeader("authorization");
        log.info("token:{}", token);
        if (StrUtil.isBlank(token)) {
            // 2.不存在，拦截，返回401
            response.setStatus(401);
            return false;
        }
        String KEY = RedisConstants.LOGIN_USER_KEY + token;
        // 2.基于token获取redis中的用户
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash()
                                                         .entries(KEY);
        // 3.判断用户是否存在
        if (userMap.isEmpty()) {
            // 4.不存在，拦截，返回401状态码
            response.setStatus(401);
            return false;
        }
        // 5.将查询到的Hash数据转为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        // 6.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(userDTO);
        // 7.刷新token有效期
        stringRedisTemplate.expire(KEY, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        // 8.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {

    }
}
