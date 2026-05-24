package io.github.maximvegorov.outbox.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;

@RequiredArgsConstructor
@Slf4j
public class OutboxSchemaInitializer implements InitializingBean {
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterPropertiesSet() {
        log.info("Initializing outbox schema");
        var schema = new ClassPathResource("io/github/maximvegorov/outbox/schema.sql");
        jdbcTemplate.execute((Connection connection) -> {
            ScriptUtils.executeSqlScript(connection, schema);
            return null;
        });
        log.info("Completed outbox schema initialization");
    }
}
