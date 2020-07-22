package org.springframework.cloud.stream.function;

import com.xiaomai.event.config.EventBindingServiceProperties;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.FunctionRegistry;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.config.BinderFactoryAutoConfiguration;
import org.springframework.cloud.stream.config.BindingBeansRegistrar;
import org.springframework.cloud.stream.config.BindingServiceConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

/**
 * @author baihe Created on 2020/7/22 9:30 PM
 */
@Configuration
@EnableConfigurationProperties({StreamFunctionProperties.class, EventBindingServiceProperties.class})
@Import({ BindingBeansRegistrar.class, BinderFactoryAutoConfiguration.class })
@AutoConfigureBefore(BindingServiceConfiguration.class)
@AutoConfigureAfter(ContextFunctionCatalogAutoConfiguration.class)
@ConditionalOnBean(FunctionRegistry.class)
public class EventFunctionConfiguration {
    @Bean
    @Primary
    public StreamBridge eventStreamBridgeUtils(
        FunctionCatalog functionCatalog, FunctionRegistry functionRegistry,
        EventBindingServiceProperties bindingServiceProperties, ConfigurableApplicationContext applicationContext) {
        return new StreamBridge(functionCatalog, functionRegistry, bindingServiceProperties, applicationContext);
    }
}
