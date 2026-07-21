package io.rapa.assetboxconsumer.email.service;

import io.rapa.assetboxconsumer.common.constants.ErrorCode;
import io.rapa.assetboxconsumer.common.exception.BusinessException;
import io.rapa.assetboxconsumer.email.dto.SendMessageDto;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

@Service
@Transactional
@RequiredArgsConstructor
public class EmailService {
    private final JavaMailSender javaMailSender;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate kafkaTemplate;
    private final RedisTemplate redisTemplate;
    private final String TOPIC_DLQ_NAME = "email-topic-dlq";
    @Value("${spring.mail.username}")
    private String senderEmail;
    private MimeMessage createMail(
            SendMessageDto dto
    ) throws MessagingException {
        MimeMessage message = javaMailSender.createMimeMessage();
        message.setFrom(senderEmail);
        message.setRecipients(
                MimeMessage.RecipientType.TO,
                dto.email()
        );
        message.setSubject("[Asset Box] 이메일 인증을 완료해 주세요", "UTF-8");

        String verificationUrl = "%s/api/email/verify?token=%s".formatted(
                normalizeBaseUrl(dto.baseUrl()),
                dto.token()
        );
        String body = """
                <!doctype html>
                <html lang="ko">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Asset Box 이메일 인증</title>
                </head>
                <body style="margin:0; padding:0; background-color:#f4f6f8; font-family:Arial, 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif; color:#1f2937;">
                    <table role="presentation" width="100%%" cellspacing="0" cellpadding="0" style="background-color:#f4f6f8; padding:40px 16px;">
                        <tr>
                            <td align="center">
                                <table role="presentation" width="100%%" cellspacing="0" cellpadding="0" style="max-width:560px; background-color:#ffffff; border:1px solid #e5e7eb; border-radius:8px; overflow:hidden;">
                                    <tr>
                                        <td style="padding:28px 32px; background-color:#111827;">
                                            <div style="font-size:20px; font-weight:700; color:#ffffff; letter-spacing:0;">Asset Box</div>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td style="padding:36px 32px 24px;">
                                            <h1 style="margin:0 0 16px; font-size:24px; line-height:1.35; color:#111827;">이메일 인증을 완료해 주세요</h1>
                                            <p style="margin:0 0 24px; font-size:15px; line-height:1.7; color:#4b5563;">
                                                Asset Box 계정 보호를 위해 이메일 주소 확인이 필요합니다.
                                                아래 버튼을 눌러 인증을 완료해 주세요.
                                            </p>
                                            <table role="presentation" cellspacing="0" cellpadding="0" style="margin:0 0 28px;">
                                                <tr>
                                                    <td bgcolor="#2563eb" style="border-radius:6px;">
                                                        <a href="%s" target="_blank" style="display:inline-block; padding:14px 24px; font-size:15px; font-weight:700; color:#ffffff; text-decoration:none;">이메일 인증하기</a>
                                                    </td>
                                                </tr>
                                            </table>
                                            <p style="margin:0 0 12px; font-size:13px; line-height:1.6; color:#6b7280;">
                                                버튼이 동작하지 않는 경우 아래 주소를 브라우저에 복사해 접속해 주세요.
                                            </p>
                                            <p style="margin:0; padding:12px 14px; background-color:#f9fafb; border:1px solid #e5e7eb; border-radius:6px; font-size:12px; line-height:1.6; color:#374151; word-break:break-all;">
                                                <a href="%s" target="_blank" style="color:#2563eb; text-decoration:none;">%s</a>
                                            </p>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td style="padding:24px 32px 32px; border-top:1px solid #e5e7eb;">
                                            <p style="margin:0 0 8px; font-size:13px; line-height:1.6; color:#6b7280;">
                                                본인이 요청하지 않은 메일이라면 이 메시지를 무시해 주세요.
                                            </p>
                                            <p style="margin:0; font-size:12px; line-height:1.6; color:#9ca3af;">
                                                이 메일은 발신 전용입니다. Asset Box 서비스를 이용해 주셔서 감사합니다.
                                            </p>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>
                </body>
                </html>
                """.formatted(
                verificationUrl,
                verificationUrl,
                verificationUrl
        );
        message.setText(body, "UTF-8", "html");
        return message;
    }
    private String normalizeBaseUrl(String baseUrl) {
        String normalizedUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        if (normalizedUrl.startsWith("http://") || normalizedUrl.startsWith("https://")) {
            return normalizedUrl;
        }
        return "http://" + normalizedUrl;
    }

    public void sendSimpleMessage(
            SendMessageDto dto
    ) throws MessagingException {
        MimeMessage message = createMail(dto);
        try {
            javaMailSender.send(message);
        } catch (MailException e) {
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
    ) {
        try {
            SendMessageDto dto = objectMapper.readValue(
                    message,
                    new TypeReference<SendMessageDto>() {
                    }
            );

            String processedKey = "email:message:processed:" + dto.requestId();

            Boolean firstProcessing = redisTemplate.opsForValue().setIfAbsent(
                    processedKey,
                    "processedId",
                    1,
                    TimeUnit.DAYS
            );

            if (!Boolean.TRUE.equals(firstProcessing)) return;

            sendSimpleMessage(dto);
            ack.acknowledge();
        } catch (Exception e) {
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
    ) {
        try {
            SendMessageDto dto = objectMapper.readValue(
                    message,
                    new TypeReference<SendMessageDto>() {
                    }
            );
            sendSimpleMessage(dto);
            ack.acknowledge();
        } catch (Exception e) {
            throw new BusinessException(ErrorCode.KAFKA_SEND_FAIL);
        }
    }
}
