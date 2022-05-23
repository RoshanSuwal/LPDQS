package org.ekbana.minikafka.client.producer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.ekbana.minikafka.client.common.KafkaServerClient;
import org.ekbana.minikafka.client.common.RequestType;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaProducer extends KafkaServerClient {

    public interface ProducerEventListener{
        void onProducerRecordWriteCompleted();
        void onProducerConnectionClose();
        void onProducerConfigurationSuccess();
    }

    private final Properties properties;
    private ServerState serverState;
    private final AtomicBoolean isIdeal;

    private final AtomicBoolean hasProducerRecord;
    private final AtomicBoolean stopAfterCompletion;

    private final BlockingDeque<String> blockingDeque;
    private final int batchSize=500;
    private ProducerEventListener producerEventListener;

    private long requestId=0;

    // batching of records

    public KafkaProducer(Properties properties) {
        this(properties,null);
    }

    public KafkaProducer(Properties properties,ProducerEventListener producerEventListener){
        super(properties.getProperty("kafka.server.address","localhost"), Integer.parseInt(properties.getProperty("kafka.server.port","9999")));
        this.properties = properties;
        this.serverState=ServerState.NOT_CONNECTED;
        isIdeal=new AtomicBoolean(true);
        hasProducerRecord=new AtomicBoolean(false);
        stopAfterCompletion=new AtomicBoolean(false);
        blockingDeque=new LinkedBlockingDeque<>(10);
        this.producerEventListener=producerEventListener;
    }

    public void setEventListener(ProducerEventListener producerEventListener){
        this.producerEventListener=producerEventListener;
    }

    public String  getAuthRequest(){
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.AUTH));
        jsonObject.addProperty("username",properties.getProperty("kafka.auth.username","user"));
        jsonObject.addProperty("password",properties.getProperty("kafka.auth.password","password"));

        return jsonObject.toString();
    }

    public String getConfigurationRequest(){
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.PRODUCER_CONFIG));
        jsonObject.addProperty("topicName",properties.getProperty("kafka.topic.name"));
        return jsonObject.toString();
    }

    private JsonArray prepareProduceRecords(){
        final JsonArray jsonElements = new JsonArray();
        int recordSize=0;
        while (blockingDeque.size()>0 && recordSize < batchSize ) {
            final String poll = blockingDeque.poll();
            jsonElements.add(poll);
            recordSize=poll.length();
        }
        if (blockingDeque.size()==0) hasProducerRecord.set(false);
        return jsonElements;
    }

    public String getProducerRecordWriteRequest(){
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.PRODUCER_RECORD_WRITE));
        jsonObject.addProperty("topicName",properties.getProperty("kafka.topic.name"));
        jsonObject.addProperty("key",properties.getProperty("kafka.topic.key",""));
        jsonObject.addProperty("partitionId",Integer.parseInt(properties.getProperty("kafka.topic.partition","-1")));
        jsonObject.add("producerRecords",prepareProduceRecords());
        jsonObject.addProperty("requestId",getRequestId());
        return jsonObject.toString();
    }

    private Long getRequestId(){
        requestId=requestId+1;
        return requestId;
    }

    public void send(String msg){
        try {
            blockingDeque.put(msg);
            hasProducerRecord.set(true);
            sendToServer();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stopAfterCompletion(){
        stopAfterCompletion.set(true);
    }

    private void sendToServer(){
        if (isIdeal.get()) {
            isIdeal.set(false);
            try {
                if (serverState == ServerState.CONNECTED) write(getConfigurationRequest());
//                else if (serverState == ServerState.AUTHENTICATED) write(getConfigurationRequest());
                else if (serverState == ServerState.CONFIGURED && hasProducerRecord.get()) write(getProducerRecordWriteRequest());
                else if (serverState==ServerState.CLOSE) close();
                else if (stopAfterCompletion.get()) close();
                else isIdeal.set(true);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                serverState=ServerState.CLOSE;
                close();
            }
        }
    }

    @Override
    protected void onConnect() {
        System.out.println("Connected to server successfully");
    }

    @Override
    protected void onRead(String readData) {

        final JsonObject readJson = new Gson().fromJson(readData, JsonObject.class);
        final RequestType requestType = RequestType.valueOf(readJson.get("requestType").getAsString());
        if (readJson.get("responseType").getAsString().equals("SUCCESS")){
            if (requestType==RequestType.NEW_CONNECTION) serverState=ServerState.CONNECTED;
            else if (requestType==RequestType.AUTH) serverState=ServerState.AUTHENTICATED;
            else if (requestType==RequestType.PRODUCER_CONFIG) {
                serverState = ServerState.CONFIGURED;
                if (producerEventListener!=null)producerEventListener.onProducerConfigurationSuccess();
            }
            else if (requestType==RequestType.PRODUCER_RECORD_WRITE) {
                if (!hasProducerRecord.get()) if (producerEventListener!=null)producerEventListener.onProducerRecordWriteCompleted();
            }
        }else {
            if (requestType==RequestType.AUTH) serverState=ServerState.CLOSE;
            else if (requestType==RequestType.PRODUCER_CONFIG) serverState=ServerState.CLOSE;
            else if (requestType==RequestType.PRODUCER_RECORD_WRITE) serverState=ServerState.CLOSE;
        }
        System.out.println(readData);
        isIdeal.set(true);
        sendToServer();
    }

    @Override
    protected void onClose() {
        System.out.println("connection closed");
        if (producerEventListener==null) System.exit(0);
        else producerEventListener.onProducerConnectionClose();
    }

    @Override
    protected void onSend(String message) {
        System.out.println(message);
    }

    public static void main(String[] args) throws IOException {
        Properties properties=new Properties();
        properties.setProperty("kafka.server.address","localhost");
        properties.setProperty("kafka.server.port","9999");
        properties.setProperty("kafka.topic.name","test");
//        properties.setProperty("kafka.topic.partition","0");
        KafkaProducer kafkaProducer=new KafkaProducer(properties);
        kafkaProducer.connect();

        for (int i=0;i<7;i++) {
            kafkaProducer.send("hello world");
            kafkaProducer.send("second message");
        }
//        System.exit(0);
        kafkaProducer.stopAfterCompletion();
    }
}