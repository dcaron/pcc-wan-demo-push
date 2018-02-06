package io.pivotal.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.gemfire.client.ClientRegionFactoryBean;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.codearte.jfairy.Fairy;
import io.codearte.jfairy.producer.person.Person;
import io.pivotal.domain.Customer;
import io.pivotal.repo.pcc.CustomerRepository;

@RestController
public class CustomerController {
	
	@Autowired
	CustomerRepository pccCustomerRepository;
	
	@Autowired
	ClientRegionFactoryBean<String, Customer> customerRegionFactory;
	
	@Autowired 
	GemFireCache clientCache;
	
	boolean CONTINUE_LOAD = false;
	int STARTING_INDEX = 0;
	
	Fairy fairy = Fairy.create();
	
	@Autowired
    private SimpMessagingTemplate webSocket;
	
	@RequestMapping(method = RequestMethod.GET, path = "/clearcache")
	@ResponseBody
	public String clearCache() throws Exception {
		Region<String, Customer> customerRegion = customerRegionFactory.getObject();
		customerRegion.removeAll(customerRegion.keySetOnServer());
		return "Region cleared";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/countcache")
	@ResponseBody
	public Long countCache() throws Exception {
		return pccCustomerRepository.count();
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/startload")
	@ResponseBody
	public String startLoad(@RequestParam(value = "region", required = true) String region, @RequestParam(value = "starting_index", required = true) String index, @RequestParam(value = "amount", required = true) String amount) throws Exception {
		
		CONTINUE_LOAD = true;
		STARTING_INDEX = Integer.parseInt(index);

		LoadWorker worker = new LoadWorker(region, Integer.parseInt(amount));
		worker.start();
		
		return "New customers successfully saved into Cloud Cache";
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/stopload")
	@ResponseBody
	public String stopLoad() throws Exception {
		
		CONTINUE_LOAD = false;
		
		return "Loading process stopped";
		
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/loadstatus")
	@ResponseBody
	public String getStatus() throws Exception {
		
		return CONTINUE_LOAD ? "running" : "stopped";
		
	}
	
	@RequestMapping(method = RequestMethod.GET, path = "/systemid")
	@ResponseBody
	public String getId() throws Exception {
		return null;
	}
	
	class LoadWorker extends Thread {
		
		String REGION_NAME;
		int BATCH_NUM;
		
		LoadWorker(String region, int num) {
			this.REGION_NAME = region;
			this.BATCH_NUM = num;
		}

	    @Override
	    public void run() {
	        
	        while (CONTINUE_LOAD) {
	        	List<Customer> customers = new ArrayList<>();
				
				for (int i=0; i<BATCH_NUM; i++) {
					Person person = fairy.person();
					Customer customer = new Customer(person.fullName(), person.email(), person.getAddress().toString(), person.dateOfBirth().toString(), REGION_NAME, STARTING_INDEX++);
					customers.add(customer);
				}
				
				pccCustomerRepository.save(customers);
				
				webSocket.convertAndSend("/topic/update", "update");
				
	            try {
	                Thread.sleep(5000);
	            } catch (InterruptedException e) {
	                break;
	            }
	        }
	    }

	}

	
}
