package io.pivotal.config;

import javax.sql.DataSource;

import io.pivotal.domain.Customer;
import io.pivotal.listener.CustomerListener;
import io.pivotal.spring.cloud.service.gemfire.GemfireServiceConnectorConfig;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.cloud.config.java.AbstractCloudConfig;
import org.springframework.cloud.service.ServiceConnectorConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.gemfire.client.ClientRegionFactoryBean;
import org.springframework.data.gemfire.support.GemfireCacheManager;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

@Configuration
@Profile("cloud")
public class DemoCloudConfig extends AbstractCloudConfig {
	
	@Autowired 
	ClientCache clientCache;
	
	public ServiceConnectorConfig createGemfireConnectorConfig() {

        GemfireServiceConnectorConfig gemfireConfig = new GemfireServiceConnectorConfig();
        gemfireConfig.setPoolSubscriptionEnabled(true);
        gemfireConfig.setPdxSerializer(new ReflectionBasedAutoSerializer(".*"));
        gemfireConfig.setPdxReadSerialized(false);

        return gemfireConfig;
    }
    
	@Bean(name = "gemfireCache")
    public ClientCache getGemfireClientCache() throws Exception {		
		
		Cloud cloud = new CloudFactory().getCloud();
		ClientCache clientCache = cloud.getSingletonServiceConnector(ClientCache.class,  createGemfireConnectorConfig());

        return clientCache;
    }


	@SuppressWarnings("unchecked")
	@Bean(name = "customer")
	public ClientRegionFactoryBean<String, Customer> customerRegion(@Autowired CustomerListener customerListener) {
		ClientRegionFactoryBean<String, Customer> customerRegionFactory = new ClientRegionFactoryBean<>();
		customerRegionFactory.setCache(clientCache);
		customerRegionFactory.setShortcut(ClientRegionShortcut.PROXY);
		customerRegionFactory.setName("customer");
		
		CacheListener<String, Customer>[] cacheListeners = new CacheListener[]{customerListener};
		customerRegionFactory.setCacheListeners(cacheListeners);

		return customerRegionFactory;
	}

}
