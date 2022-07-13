package com.philips.hc.iap.svc.digitaltwin.service;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class DXREventReceiver {

    @Autowired
    private EventParser evParser;

    public void consumeEventsByConsumerGroup(){
        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString("Endpoint=sb://dxrddwinstancegrfeventhub.servicebus.windows.net/;SharedAccessKeyName=dxrdwinstancegrfeventhubsinkpolicy;SharedAccessKey=0u3Hdy9HiVq8J2ty2XJqszaty5+pWhViSbsVP4tnm3I=;EntityPath=dxrdwinstancegrfeventhubsink")
                //    .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .consumerGroup("dxrdwinstancesinkconsumergroup")
                .buildAsyncConsumerClient();

// Receive newly added events from partition with id "0". EventPosition specifies the position
// within the Event Hub partition to begin consuming events.
        consumer.receiveFromPartition("0", EventPosition.latest()).subscribe(event -> {
            // Process each event as it arrives.
            System.out.println("EVENT DATA :: "+event.getData().getBodyAsString());
            evParser.parseEvent(event.getData().getBodyAsString());
        });
// add sleep or System.in.read() to receive events before exiting the process.
    }

    public void test(){

        //     EventParser evParser = new EventParser();

        evParser.parseEvent("{\n" +
                "  \"dtwin_id\": \"dtmi:com:philips:pd:dxr:portable_detector;2\",\n" +
                "  \"eventDateTime\": \"2022-06-07T14:30:37.944Z\",\n" +
                "  \"wifilinkQuality\": 2,\n" +
                "  \"wifirawLinkQuality\": 100,\n" +
                "  \"wifidataRate\": 162,\n" +
                "  \"wifinoisePower\": -81,\n" +
                "  \"wifisignalPower\": -66,\n" +
                "  \"instanceType\": \"Detector\",\n" +
                "  \"dtwin_instance_id\": \"Detector-SN1604W3\"\n" +
                "}");





    }

    @PostConstruct
    public void init(){
        // initialize your monitor here, instance of someService is already injected by this time.

        System.out.println("Inside init");
      //   test();
     //   consumeEventsByConsumerGroup();
        consumeEventsByConsumerGroup();
    }
    }
