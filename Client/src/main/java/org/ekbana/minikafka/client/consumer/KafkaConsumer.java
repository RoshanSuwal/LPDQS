package org.ekbana.minikafka.client.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.ekbana.minikafka.client.common.KafkaServerClient;
import org.ekbana.minikafka.client.common.RequestType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumer extends KafkaServerClient {

    private final Properties properties;
    private ServerState serverState;

    private final AtomicBoolean stopAfterCompletion;

    private JsonObject topicJson;
    private Map<Integer, Boolean> partitionRequestStatus;
    private Map<Integer, String> partitionRequest;

    private BlockingDeque<String> requestQueue;
    private BlockingDeque<String> readRequestQueue;
    private BlockingDeque<JsonObject> consumerRecordsQueue;

    private AtomicBoolean isRequesting;

    public KafkaConsumer(Properties properties) {
        super(properties.getProperty("kafka.server.address", "localhost"), Integer.parseInt(properties.getProperty("kafka.server.port", "9999")));
        this.properties = properties;
        this.serverState = ServerState.NOT_CONNECTED;
        this.stopAfterCompletion = new AtomicBoolean(true);
        requestQueue = new LinkedBlockingDeque<>(10);
        readRequestQueue=new LinkedBlockingDeque<>();
        consumerRecordsQueue = new LinkedBlockingDeque<>(10);
        isRequesting=new AtomicBoolean(false);
    }

    public String getAuthRequest() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.AUTH));
        jsonObject.addProperty("username", properties.getProperty("kafka.auth.username", "user"));
        jsonObject.addProperty("password", properties.getProperty("kafka.auth.password", "password"));

        return jsonObject.toString();
    }

    public String getConsumerConfigurationRequest() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.CONSUMER_CONFIG));
        jsonObject.addProperty("topicName", properties.getProperty("kafka.topic.name"));
        jsonObject.addProperty("groupName", properties.getProperty("kafka.consumer.group", "0"));
        return jsonObject.toString();
    }

    public String getCommitOffsetRequest(int partitionId, long offset) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.CONSUMER_OFFSET_COMMIT));
        jsonObject.addProperty("topicName", properties.getProperty("kafka.topic.name"));
        jsonObject.addProperty("groupName", properties.getProperty("kafka.consumer.group", "0"));
        jsonObject.addProperty("partitionId", partitionId);
        jsonObject.addProperty("offset", offset);
        return jsonObject.toString();
    }

    public String getConsumerRecordReadRequest(int partitionId, long offset, boolean isTimeOffset) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.CONSUMER_RECORD_READ));
        jsonObject.addProperty("topicName", properties.getProperty("kafka.topic.name"));
        jsonObject.addProperty("groupName", properties.getProperty("kafka.consumer.group.name", "0"));
        jsonObject.addProperty("partitionId", partitionId);
        jsonObject.addProperty("offset", offset);
        jsonObject.addProperty("isTimeOffset", isTimeOffset);
        return jsonObject.toString();
    }

    private void sendRecordReadRequestToServer(int partitionId, String readRecordRequest) {
        // has internal queue of request
        partitionRequestStatus.put(partitionId, true);
        partitionRequest.put(partitionId, readRecordRequest);
        readRequestQueue.add(readRecordRequest);
//        sendToServer(readRecordRequest);
    }

    private String getReadRequest() {
        return readRequestQueue.pollFirst();
    }

    private void sendToServer(String request) {
        if (request!=null) requestQueue.add(request);
        if (!isRequesting.get()) {
            final String polledRequest = requestQueue.poll();
            try {
                isRequesting.set(true);
                if (polledRequest != null) {
                    write(polledRequest);
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                isRequesting.set(false);
                serverState = ServerState.CLOSE;
                close();
            }
        }
    }

    private void consumerConfigured(JsonObject topicJson) {
        this.topicJson = topicJson;
        this.partitionRequestStatus = new HashMap<>();
        this.partitionRequest = new HashMap<>();
        final int numberOfPartitions = topicJson.get("numberOfPartitions").getAsInt();
        final String partitions = properties.getProperty("kafka.consumer.group.partition", "-1");
        final long[] offsets = Arrays.stream(properties.getProperty("kafka.consumer.group.offsets", "0").strip().split(",")).mapToLong(Long::parseLong).toArray();
        final boolean isTimeOffset = Boolean.parseBoolean(properties.getProperty("kafka.consumer.group.offset.time", "false"));
        if (partitions.equals("-1")) {
            for (int i = 0; i < numberOfPartitions; i++) {
                final long offset = offsets.length > i ? offsets[i] : 0;
                sendRecordReadRequestToServer(i, getConsumerRecordReadRequest(i, offset, isTimeOffset));
            }
        }
        sendNextRequestToServer();
    }

    @Override
    protected void onConnect() {
        System.out.println("Connected to server successfully");
    }

    @Override
    protected void onRead(String readData) {
        isRequesting.set(false);
        System.out.println(readData);
        final JsonObject readJson = new Gson().fromJson(readData, JsonObject.class);
        final RequestType requestType = RequestType.valueOf(readJson.get("requestType").getAsString());
        if (readJson.get("responseType").getAsString().equals("SUCCESS")) {
            if (requestType == RequestType.NEW_CONNECTION) {
                serverState = ServerState.CONNECTED;
//                sendToServer(getAuthRequest());
                sendToServer(getConsumerConfigurationRequest());
            } else if (requestType == RequestType.AUTH) {
                serverState = ServerState.AUTHENTICATED;
                sendToServer(getConsumerConfigurationRequest());
            } else if (requestType == RequestType.CONSUMER_CONFIG) {
                serverState = ServerState.CONFIGURED;
                consumerConfigured(readJson.getAsJsonObject("topic"));
            } else if (requestType == RequestType.CONSUMER_RECORD_READ) {
                // process the response
                final JsonObject consumerRecords = readJson.get("consumerRecords").getAsJsonObject();
                final int partitionId = consumerRecords.get("partitionId").getAsInt();
                final long offset = consumerRecords.get("endingOffset").getAsLong();
                partitionRequestStatus.put(partitionId, false);
                if (offset == -1) {
                    sendRecordReadRequestToServer(partitionId, partitionRequest.get(partitionId));
                    sendNextRequestToServer();
                } else {
                    sendRecordReadRequestToServer(partitionId, getConsumerRecordReadRequest(partitionId, offset+1, false));
                    consumerRecordsQueue.add(consumerRecords);
                    System.out.println("records size : "+consumerRecordsQueue.size());
                    // add data to read queue
                    // add offset commit logic
                    if (Boolean.parseBoolean(properties.getProperty("kafka.consumer.group.commit.afterDataRead", "true"))) {
                        sendNextRequestToServer();
                    } else {
                        sendOffsetCommitRequest(partitionId, offset);
                    }
                }
            } else if (requestType == RequestType.CONSUMER_OFFSET_COMMIT) {
                sendNextRequestToServer();
            }
        } else {
            if (requestType == RequestType.AUTH) serverState = ServerState.CLOSE;
            else if (requestType == RequestType.PRODUCER_CONFIG) serverState = ServerState.CLOSE;
            else if (requestType == RequestType.CONSUMER_RECORD_READ) serverState = ServerState.CLOSE;
        }

    }

    private void sendNextRequestToServer() {
        if (consumerRecordsQueue.remainingCapacity() > 0)
            sendToServer(getReadRequest());
    }

    public void sendOffsetCommitRequest(int partitionId, long offset) {
        // send to server
        // offset commit request
        sendToServer(getCommitOffsetRequest(partitionId, offset));
    }

    public JsonObject getRecords() throws InterruptedException {
        System.out.println("getting records by consumer");
        final JsonObject take = consumerRecordsQueue.take();
        System.out.println("records size after read : "+readRequestQueue.size());
        if (Boolean.parseBoolean(properties.getProperty("kafka.consumer.group.commit.afterDataRead", "true"))) {
            // send the commit request to server
            sendNextRequestToServer();
        }else {
            sendNextRequestToServer();
        }
        return take;
    }

    @Override
    protected void onClose() {
        System.out.println("Connection Closed");
    }

    @Override
    protected void onSend(String message) {
        System.out.println(message);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("kafka.topic.name", "tweets5");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.connect();

        while (true){
            final JsonObject records = kafkaConsumer.getRecords();
            System.out.println("read records : "+records);
            Thread.sleep(1000);
        }
    }
}
