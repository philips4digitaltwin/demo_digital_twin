package com.philips.hc.iap.svc.digitaltwin.service;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.*;
import com.azure.storage.blob.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.function.Consumer;
@Service
public class Receiver {

    //Event Hubs namespace connection string
    private static final String connectionString = "Endpoint=sb://digitaltwin-datastore-event-hubs.servicebus.windows.net/;SharedAccessKeyName=StreamAnalyticsJobDigitalTwin_policy;SharedAccessKey=q3yEa3fDzP3hNDcLTqSFZhUyPTozlz/+Z75SdiHmJF8=;EntityPath=digital-twin";
    //Event hub name
    private static final String eventHubName = "digital-twin";
    private static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=eventhubstoragejava;AccountKey=tFo2XW5cZ917NAXIlSwWdp4NcViYo5RVH/bS2xVkNb5cEEAaes6kgOXpwxCPB9HgtpUlVqprEBkj+AStfYYFng==;EndpointSuffix=core.windows.net";
    private static final String storageContainerName = "eventhubcontainer";

    @Autowired
    private EventParser evParser;

    private static Receiver receiver;

    public static Receiver getInstance(){
        if(receiver == null){
            receiver = new Receiver();
        }

        return receiver;
    }

    public static void main(String[] args) throws Exception {
       // consumeEventsByConsumerGroup();

//        // Create a blob container client that you use later to build an event processor client to receive and process events
//        BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
//                .connectionString(storageConnectionString)
//                .containerName(storageContainerName)
//                .buildAsyncClient();
//
//        // Create a builder object that you will use later to build an event processor client to receive and process events and errors.
//        EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
//                .connectionString(connectionString, eventHubName)
//                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
//                .processEvent(PARTITION_PROCESSOR)
//                .processError(ERROR_HANDLER)
//                .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));
//
//        // Use the builder object to create an event processor client
//        EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
//
//        System.out.println("Starting event processor");
//        eventProcessorClient.start();
//
//        System.out.println("Press enter to stop.");
//        System.in.read();
//
//        System.out.println("Stopping event processor");
//        eventProcessorClient.stop();
//        System.out.println("Event processor stopped.");
//
//        System.out.println("Exiting process");
    }

    public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
        PartitionContext partitionContext = eventContext.getPartitionContext();
        EventData eventData = eventContext.getEventData();

        System.out.printf("Processing event from partition %s with sequence number %d with body: %s%n",
                partitionContext.getPartitionId(), eventData.getSequenceNumber(), eventData.getBodyAsString());

        // Every 10 events received, it will update the checkpoint stored in Azure Blob Storage.
        if (eventData.getSequenceNumber() % 10 == 0) {
            eventContext.updateCheckpoint();
        }
    };

    public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
        System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable());
    };

    public void consumeEventsByConsumerGroup(){
        EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
                .connectionString("Endpoint=sb://adtmreventhubtestakshat.servicebus.windows.net/;SharedAccessKeyName=adtmreventhubsinkpolicy;SharedAccessKey=+j3USUT22a+Wpoh5ZaLDb4c1LWcSTSfLxEtut3cT9EY=;EntityPath=adtmreventhubsink")
            //    .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .consumerGroup("adtmreventhubconsumergrp")
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
                "\n" +
                "  \"dtwin_id\": \"dtmi:com:philips:pd:mr:lcc;5\",\n" +
                "\n" +
                "  \"LCC_GradAmpFilterPress\": 0.1,\n" +
                "\n" +
                "  \"LCC_GradAmpPress\": 5.6,\n" +
                "\n" +
                "  \"LCC_GradCoilFilterPress\": 0.1,\n" +
                "\n" +
                "  \"LCC_GradCoilPress\": 5.2,\n" +
                "\n" +
                "  \"LCC_GradCoilTemp\": 25.2,\n" +
                "\n" +
                "  \"LCC_SecondaryLoopFillingPress\": -1,\n" +
                "\n" +
                "  \"LCC_SecondaryLoopTemp\": -1,\n" +
                "\n" +
                "  \"LCC_T1Temp\": 12.2,\n" +
                "\n" +
                "  \"LCC_T2Temp\": 12.6,\n" +
                "\n" +
                "  \"LCC_GradAmpFillingPress\": -1,\n" +
                "\n" +
                "  \"LCC_GradAmpTemp\": 25.1,\n" +
                "\n" +
                "  \"LCC_GradCoilFillingPress\": -1,\n" +
                "\n" +
                "  \"LCC_HeliumCompressorFlow\": -1,\n" +
                "\n" +
                "  \"LCC_PrimaryFlow\": -1,\n" +
                "\n" +
                "  \"LCC_HCTemp\": -1,\n" +
                "\n" +
                "  \"LCC_HeliumCompressorTemp\": -1,\n" +
                "\n" +
                "  \"lcc_Type\": \"LCC\",\n" +
                "\n" +
                "  \"EventDate\": \"2022-06-28T15:06:54Z\",\n" +
                "\n" +
                "  \"dtwin_instance_id\": \"18542_LCC_twin\",\n" +
                "\n" +
                "  \"InstanceType\": \"LCC\",\n" +
                "\n" +
                "  \"SRN\": \"18542\"\n" +
                "\n" +
                "}");





        evParser.parseEvent("{\n" +
                "  \"dtwin_id\": \"dtmi:com:philips:pd:mr:magnet;2\",\n" +
                "  \"magnet_pressure\": 75.0,\n" +
                "  \"magnet_bathheaterlow\": -1.0,\n" +
                "  \"magnet_helium_level_value\": 55.8,\n" +
                "  \"magnet_quench\": 0.0,\n" +
                "  \"Magnet_Type\": \"F2000\",\n" +
                "  \"Main_System_Type\": \"T15\",\n" +
                "  \"Magnet_Serial_Number\": \"TE_150_2752\",\n" +
                "  \"Magnet_MMU_Type\": \"MEU\",\n" +
                "  \"EventDate\": \"2022-05-01T03:05:31Z\",\n" +
                "  \"dtwin_instance_id\": \"8190_Magnet_twin\",\n" +
                "  \"InstanceType\": \"Magnet\",\n" +
                "  \"SRN\": \"8190\"\n" +
                "} ");
    }

    @PostConstruct
    public void init(){
        // initialize your monitor here, instance of someService is already injected by this time.

        System.out.println("Inside init");
       // test();
        consumeEventsByConsumerGroup();
    }
}
