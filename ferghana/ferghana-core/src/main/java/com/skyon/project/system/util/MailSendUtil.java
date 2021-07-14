package com.skyon.project.system.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class MailSendUtil {

    @Autowired
    JavaMailSender jms;

    public static JavaMailSender staticJms;

    @Value("${spring.mail.username}")
    public String username;

    private static String userName;

    @PostConstruct
    public void init(){
        MailSendUtil.staticJms = jms;
        MailSendUtil.userName = username;
    }

    public static void sendMail(String mailAddress, String msgSubject, String msgText){
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(userName); // 发送者邮箱地址
        message.setTo(mailAddress); // 收件人邮箱地址
        message.setSubject(msgSubject); // 邮件主题
        message.setText(msgText); // 邮件内容
        staticJms.send(message);
    }
}
