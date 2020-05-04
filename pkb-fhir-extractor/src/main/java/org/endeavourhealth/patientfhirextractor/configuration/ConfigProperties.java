package org.endeavourhealth.patientfhirextractor.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@Data
@ConfigurationProperties("config.datasource")
public class ConfigProperties {

    String url;
    String username;
    String password;
    String useSSL;
    String className;
}


