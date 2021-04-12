package com.skyon.project.system.controller;

import com.skyon.common.constant.Constants;
import com.skyon.framework.security.LoginUser;
import com.skyon.framework.security.service.SysSsoService;
import com.skyon.framework.security.service.TokenService;
import com.skyon.framework.web.domain.SsoResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.skyon.framework.web.domain.AjaxResult;

import javax.servlet.http.HttpServletRequest;

/**
 * 单点登录
 *
 * @author zhouminfeng
 */
@RestController
@RequestMapping("/sso")
public class SysSsoController {
    private static final Logger log = LoggerFactory.getLogger(SysSsoController.class);
    @Autowired
    private SysSsoService sysSsoService;
    @Autowired
    private UserDetailsService userDetailsService;
    @Autowired
    private TokenService tokenService;


    /**
     * 单点登录 获取token
     *
     * @param appId       调用者标识，门户系统标识为”portal”
     * @param accessPwd   双方提前定义好一个密码
     * @param userAccount 需要获取loginToken的用户账号
     * @return
     */
    @RequestMapping("/loginToken")
    public SsoResult getLoginToken(String appId, String accessPwd, String userAccount) {
        log.info("------------url:[{}],密码：[{}],账户：[{}]", appId, accessPwd, userAccount);
        // 校验用户名
        UserDetails userDetails = userDetailsService.loadUserByUsername(userAccount);
        PreAuthenticatedAuthenticationToken authentication =
                new PreAuthenticatedAuthenticationToken(userDetails,
                        userDetails.getPassword(),
                        userDetails.getAuthorities());

        //设置authentication中details
//		authentication.setDetails(new WebAuthenticationDetails(request));

        //存放authentication到SecurityContextHolder
        SecurityContextHolder.getContext().setAuthentication(authentication);

        AjaxResult ajax = AjaxResult.success();
        // 生成令牌
        LoginUser loginUser = (LoginUser) authentication.getPrincipal();
        // 生成token set redis | key:login_tokens:uuid  | value: user信息 | 过期时间30分钟
        String token = tokenService.createToken(loginUser);
        ajax.put(Constants.TOKEN, token);
        return sysSsoService.getLoginToken(appId, accessPwd, userAccount);
    }

    @RequestMapping("/login")
    public AjaxResult login(HttpServletRequest request,String userAccount, String loginToken) {
        log.info("+++++++++++++++++++用户名：[{}],检验Token：[{}]",userAccount,loginToken);
        return sysSsoService.ssoLogin(request,userAccount, loginToken);
    }
}
