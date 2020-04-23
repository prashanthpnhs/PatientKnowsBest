package org.endeavourhealth.patientfhirextractor.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.IOException;


@Configuration
public class DataSourceConfig {

    @Autowired
    ExporterProperties exporterProperties;

    @Bean
    public DataSource getDataSource() throws IOException {
        JsonNode json = getConfig();
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("com.mysql.cj.jdbc.Driver");
        dataSourceBuilder.url(json.get("url").asText());
        dataSourceBuilder.username(json.get("username").asText());
        dataSourceBuilder.password(json.get("password").asText());
        return dataSourceBuilder.build();
    }

    public JsonNode getConfig() throws IOException {
        return ConfigManager.getConfigurationAsJson("database",exporterProperties.getConfig());
    }


}
