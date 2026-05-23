package io.github.maximvegorov.outbox.internal;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;

@RequiredArgsConstructor
public class OutboxSchemaInitializer implements InitializingBean {
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        var schema = new ClassPathResource("io/github/maximvegorov/outbox/schema.sql");
        jdbcTemplate.execute((Connection connection) -> {
            ScriptUtils.executeSqlScript(connection, schema);
            return null;
        });
    }
}
