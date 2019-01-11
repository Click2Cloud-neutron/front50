package com.netflix.spinnaker.front50.config;

import com.aliyun.oss.OSS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.front50.model.OssStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import com.netflix.spinnaker.front50.CommonStorageServiceDAOConfig;

@Configuration
@ConditionalOnExpression("${spinnaker.oss.enabled:false}")
@EnableConfigurationProperties(OssProperties.class)

public class OssConfig extends CommonStorageServiceDAOConfig {
  @Bean
  @ConditionalOnMissingBean(RestTemplate.class)
  public RestTemplate restTemplate() {
    return new RestTemplate();
  }

  @Bean
  public OssStorageService ossStorageService(OSS aliyunOss, OssProperties ossProperties) {
    ObjectMapper ossObjectMapper = new ObjectMapper();
    OssStorageService service = new OssStorageService(
      ossObjectMapper,
      aliyunOss,
      ossProperties.getBucket(),
      ossProperties.getRootFolder(),
      ossProperties.isFailoverEnabled(),
      ossProperties.getRegion(),
      ossProperties.getMaxKeys()
    );
    service.ensureBucketExists();

    return service;
  }
}
