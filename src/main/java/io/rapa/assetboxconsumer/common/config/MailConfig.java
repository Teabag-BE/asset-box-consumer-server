package io.rapa.assetboxconsumer.common.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

@Configuration
public class MailConfig {
    @Value("${spring.mail.host}")
    private String host; // Gmail SMTP 서버의 호스트 주소 : smtp.gmail.com
    @Value("${spring.mail.port}")
    private int port; // Gmail SMTP 서버가 사용하는 포트번호 : 587
    @Value("${spring.mail.username}")
    private String username; // 발신자 이메일
    @Value("${spring.mail.password}")
    private String password; // 앱 16자 비밀번호
    @Bean
    public JavaMailSender javaMailSender(){
        // JavaMailSender 구현체 생성
        JavaMailSenderImpl javaMailSender = new JavaMailSenderImpl();
        javaMailSender.setHost(host);
        javaMailSender.setPort(port);
        javaMailSender.setUsername(username);
        javaMailSender.setPassword(password);
        // 추가 속성 설정을 위한 객체 생성
        Properties props = javaMailSender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp"); // 프로토콜 : SMTP로 설정
        props.put("mail.smtp.auth", "true"); // SMTP 인증 사용
        props.put("mail.smtp.starttls.enable", "true"); // TLS 암호화 사용
        props.put("mail.debug", "true"); // 디버깅 활성화 - 메일 전송 시 로그출력
        return javaMailSender;
    }
}
