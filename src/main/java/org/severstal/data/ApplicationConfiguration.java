package org.severstal.data;

import com.clickhouse.jdbc.ClickHouseDataSource;
import org.severstal.data.config.ClickhouseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class ApplicationConfiguration {

    private final ClickhouseConfig cfg;

    public ApplicationConfiguration(@Autowired ClickhouseConfig cfg) {
        this.cfg = cfg;
    }

    @Bean()
    @Scope("singleton")
    public Connection connection() throws SQLException {
        Connection conn;
        Properties properties = new Properties();
        ClickHouseDataSource dataSource = new ClickHouseDataSource(cfg.getUrl(), properties);
        conn = dataSource.getConnection(cfg.getUsername(), cfg.getPassword());

        return conn;
    }
}
