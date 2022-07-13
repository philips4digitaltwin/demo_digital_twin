package com.philips.hc.iap.svc.digitaltwin;

import com.philips.hc.iap.svc.digitaltwin.service.EventParser;
import com.philips.hc.iap.svc.digitaltwin.service.Receiver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.philips.hc.iap.svc.digitaltwin.service")
public class DemoDigitalTwinApplication {
    @Autowired
	Receiver rc;

	@Autowired
	EventParser evParser;
	public static void main(String[] args) {
		SpringApplication.run(DemoDigitalTwinApplication.class, args);
	//	Receiver.consumeEventsByConsumerGroup();
	//	Receiver.getInstance().test();
		//rc.test();
	}

	//public

}
