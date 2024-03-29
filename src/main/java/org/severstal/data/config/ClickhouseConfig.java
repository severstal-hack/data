package org.severstal.data.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "clickhouse")
@Scope("singleton")
public class ClickhouseConfig {
    private String url;
    private String username;
    private String password;
}
