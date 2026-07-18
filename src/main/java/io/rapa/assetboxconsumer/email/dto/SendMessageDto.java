package io.rapa.assetboxconsumer.email.dto;

public record SendMessageDto(
        String email,
        String baseUrl,
        String token
) {
}
