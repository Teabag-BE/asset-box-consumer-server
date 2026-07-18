package io.rapa.assetboxconsumer.email.service;

import io.rapa.assetboxconsumer.common.constants.ErrorCode;
import io.rapa.assetboxconsumer.common.exception.BusinessException;
import io.rapa.assetboxconsumer.email.dto.SendMessageDto;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

@Service
@Transactional
@RequiredArgsConstructor
public class EmailService {
    private final JavaMailSender javaMailSender;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate kafkaTemplate;

    private final String TOPIC_DLQ_NAME = "email-topic-dlq";

    @Value("${spring.mail.username}")
    private static String senderEmail;

    // 메일 내용을 생성하는 메서드
    private MimeMessage createMail(
            SendMessageDto dto
    ) throws MessagingException {
        MimeMessage message = javaMailSender.createMimeMessage();
        // 발신자 이메일 주소 설정
        message.setFrom(senderEmail);
        // 수신자 이메일 주소 설정
        message.setRecipients(
                MimeMessage.RecipientType.TO,
                dto.email()
        );
        // 이메일 제목 설정
        message.setSubject("이메일 인증 링크");
        //
        // 본문 내용
        String body = "";
        body += "<h3>이메일 인증 링크입니다.</h3>";
        body += "<a href=\"http://%s/api/email/verify?token=%s".formatted(
                dto.baseUrl(),
                dto.token()
        ) + "\">여기를 클릭하여 인증하세요</a>";
        body += "<p>감사합니다.</p>";
        //
        message.setText(body, "UTF-8", "html");
        return message;
    }

    // 이메일 발송 메서드
    public void sendSimpleMessage(
            SendMessageDto dto
    ) throws MessagingException {
        MimeMessage message = createMail(dto);
        try{
            javaMailSender.send(message);
        } catch (MailException e){
            e.printStackTrace();
            throw new BusinessException(ErrorCode.MAIL_SEND_FAIL);
        }
    }

    @KafkaListener(
            topics = "email-validation",
            groupId = "${spring.kafka.consumer.email-topic-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consume(
            String message,
            Acknowledgment ack
    ){
        try{
            SendMessageDto dto = objectMapper.readValue(
                    message,
                    new TypeReference<SendMessageDto>() {
                    }
            );
            sendSimpleMessage(dto);
            ack.acknowledge();
        }catch (Exception e){
            kafkaTemplate.send(
                    TOPIC_DLQ_NAME,
                    message
            );
        }
    }

    @KafkaListener(
            topics = "email-topic-dlq",
            groupId = "${spring.kafka.consumer.email-topic-group-id}",
            containerFactory = "kafkaListener"
    )
    public void consumeByDlq(
            String message,
            Acknowledgment ack
    ){
        try{
            SendMessageDto dto = objectMapper.readValue(
                    message,
                    new TypeReference<SendMessageDto>() {
                    }
            );
            sendSimpleMessage(dto);
            ack.acknowledge();
        }catch (Exception e){
            throw new BusinessException(ErrorCode.KAFKA_SEND_FAIL);
        }
    }
}
