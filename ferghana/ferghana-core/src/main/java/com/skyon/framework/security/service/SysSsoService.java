package com.skyon.framework.security.service;

import com.alibaba.fastjson.JSON;
import com.skyon.common.constant.Constants;
import com.skyon.common.constant.HttpStatus;
import com.skyon.common.enums.SsoStatus;
import com.skyon.common.enums.UserStatus;
import com.skyon.common.utils.SecurityUtils;
import com.skyon.common.utils.ServletUtils;
import com.skyon.common.utils.StringUtils;
import com.skyon.common.utils.ip.IpUtils;
import com.skyon.framework.redis.RedisCache;
import com.skyon.framework.security.LoginUser;
import com.skyon.framework.web.domain.AjaxResult;
import com.skyon.framework.web.domain.SsoResult;
import com.skyon.project.system.domain.SysSsoApp;
import com.skyon.project.system.domain.SysUser;
import com.skyon.project.system.mapper.SysSsoAppMapper;
import com.skyon.project.system.mapper.SysUserMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.TimeUnit;


@Service
public class SysSsoService {

    private static final Logger log = LoggerFactory.getLogger(SysSsoService.class);

    @Autowired
    private SysSsoAppMapper sysSsoAppMapper;
    @Autowired
    private SysUserMapper sysUserMapper;
    @Autowired
    private UserDetailsService userDetailsService;
    @Autowired
    private TokenService tokenService;
    @Autowired
    private RedisCache redisCache;

    protected static final long MILLIS_SECOND = 1000;

    protected static final long MILLIS_MINUTE = 60 * MILLIS_SECOND;

    public SsoResult getLoginToken(String appId, String accessPwd, String userAccount) {
//        SysSsoApp app = sysSsoAppMapper.findAppByAppId(appId);
        SysUser sysUser = sysUserMapper.selectUserByUserName(userAccount);
        String ip = IpUtils.getIpAddr(ServletUtils.getRequest());
        log.info("请求方系统ip==>{}", ip);
//        if (app == null) {
//            return new SsoResult(SsoStatus.invalidApp.name(), SsoStatus.invalidApp.getMessage());
//        } else if (!StringUtils.equals(accessPwd, app.getAccessPwd())) {
//            return new SsoResult(SsoStatus.invalidApp.name(), SsoStatus.invalidApp.getMessage());
//        } else if (!StringUtils.equals(ip, app.getIp())) {
//            return new SsoResult(SsoStatus.invalidIp.name(), SsoStatus.invalidIp.getMessage());
//        } else if (sysUser == null || !StringUtils.equals(UserStatus.OK.getCode(), sysUser.getStatus())) {
//            return new SsoResult(SsoStatus.invalidUserAccount.name(), SsoStatus.invalidUserAccount.getMessage());
//        }

        // 验证用户信息 并授权
        PreAuthenticatedAuthenticationToken authentication = getPreAuthenticatedAuthenticationToken(userAccount);


        LoginUser loginUser = (LoginUser) authentication.getPrincipal();
        // 生成令牌 // 生成token set redis | key:login_tokens:uuid  | value: user信息 | 过期时间30分钟
        String token = tokenService.createToken(loginUser);

        long expireTime = System.currentTimeMillis() + MILLIS_MINUTE;
        // 生成token set redis | key:用户名 |  value:token  | 过期时间 60秒
        redisCache.setCacheObject(userAccount, token, (int) MILLIS_MINUTE / 1000, TimeUnit.MINUTES);
        SsoResult ssoResult = SsoResult.success();
        ssoResult.put(Constants.TOKEN, token);
        return ssoResult;

    }

    public AjaxResult ssoLogin(HttpServletRequest request,String userAccount, String token) {
        String loginKey = userAccount;
        String cacheToken = redisCache.getCacheObject(loginKey); // 获取第二个token
        if (StringUtils.isEmpty(cacheToken)) {
            return AjaxResult.error(HttpStatus.SSO_ERROR, SsoStatus.invalidToken.getMessage());
        } else if (!StringUtils.equals(cacheToken, token)) {
            return AjaxResult.error(HttpStatus.SSO_ERROR, SsoStatus.differToken.getMessage());
        }
        redisCache.deleteObject(loginKey); // 删除第二个token



        PreAuthenticatedAuthenticationToken authentication = getPreAuthenticatedAuthenticationToken(userAccount);

        //设置authentication中details
//		authentication.setDetails(new WebAuthenticationDetails(request));
        authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

        //存放authentication到SecurityContextHolder
        SecurityContextHolder.getContext().setAuthentication(authentication);
        AjaxResult ajax = AjaxResult.success(SsoStatus.loginSucess.getMessage());
        ajax.put(Constants.TOKEN, token);
        System.out.println(JSON.toJSON(ajax).toString());


//        LoginUser user = redisCache.getCacheObject("login_tokens:1534e8f4-447d-44bd-bae5-e402cf28be58");
//        if (StringUtils.isNotNull(user) && StringUtils.isNull(SecurityUtils.getAuthentication()))
//        {
////            tokenService.verifyToken(loginUser); // 刷新redis缓存
//            UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities());
//            authenticationToken.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
//            SecurityContextHolder.getContext().setAuthentication(authenticationToken);
//        }


        return ajax;

    }

    private PreAuthenticatedAuthenticationToken getPreAuthenticatedAuthenticationToken(String userAccount) {
//        UsernamePasswordAuthenticationToken authenticationToken = new UsernamePasswordAuthenticationToken(loginUser, null, loginUser.getAuthorities());
//        authenticationToken.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
//        SecurityContextHolder.getContext().setAuthentication(authenticationToken);

        UserDetails userDetails = userDetailsService.loadUserByUsername(userAccount);
        return new PreAuthenticatedAuthenticationToken(userDetails,
                userDetails.getPassword(),
                userDetails.getAuthorities());
    }


    /**
     * 验证token是否在有效期内
     *
     * @param expireTime
     * @return
     */
    private boolean verifyTokenExpire(long expireTime) {
        long currentTime = System.currentTimeMillis();
        long interval = expireTime - currentTime;
        if (interval <= MILLIS_MINUTE && interval > 0) {
            return false;
        } else {
            return true;
        }
    }
}
