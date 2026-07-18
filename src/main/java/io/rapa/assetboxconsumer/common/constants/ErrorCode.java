package io.rapa.assetboxconsumer.common.constants;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {
    MAIL_SEND_FAIL(HttpStatus.INTERNAL_SERVER_ERROR,"메일 발송 중 오류가 발생했습니다."),
    KAFKA_SEND_FAIL(HttpStatus.INTERNAL_SERVER_ERROR,"메시지 수신 중 오류가 발생했습니다.");

    private final HttpStatus status;
    private final String description;
}
