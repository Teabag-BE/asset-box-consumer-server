package io.rapa.assetboxconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AssetBoxConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(AssetBoxConsumerApplication.class, args);
    }

}
