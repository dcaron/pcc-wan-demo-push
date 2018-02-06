package io.pivotal.listener;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import io.pivotal.domain.Customer;

@Component
public class CustomerListener extends CacheListenerAdapter<String, Customer> {
	
	Logger logger =  LoggerFactory.getLogger(this.getClass());

	@Autowired
    private SimpMessagingTemplate webSocket;
	
	@Override
	public void afterCreate (EntryEvent<String, Customer> e) {
		
		try {
			if (!e.getOperation().isLocalLoad()) {
				webSocket.convertAndSend("/topic/subscribe_log", "<b>["+e.getNewValue().getId()+"]</b> " + e.getNewValue());
			}
		} catch (Exception ex) {
			logger.info("Exception is: " + ex);
		}
		
	}
	
	@Override
	public void afterUpdate(EntryEvent<String, Customer> e) {
	
	}
	
}
