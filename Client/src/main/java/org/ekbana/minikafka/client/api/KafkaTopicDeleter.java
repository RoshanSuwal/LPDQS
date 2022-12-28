package org.ekbana.minikafka.client.api;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.ekbana.minikafka.client.common.KafkaServerClient;
import org.ekbana.minikafka.client.common.RequestType;

import java.io.IOException;
import java.util.Properties;

public class KafkaTopicDeleter extends KafkaServerClient {

    private final Properties properties;
    private KafkaServerClient.ServerState serverState;

    public KafkaTopicDeleter(Properties properties) {
        super(properties.getProperty("kafka.server.address", "localhost"), Integer.parseInt(properties.getProperty("kafka.server.port", "9999")));
        this.properties = properties;
        this.serverState = KafkaServerClient.ServerState.NOT_CONNECTED;
    }

    public String getAuthRequest() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.AUTH));
        jsonObject.addProperty("username", properties.getProperty("kafka.auth.username", "user"));
        jsonObject.addProperty("password", properties.getProperty("kafka.auth.password", "password"));

        return jsonObject.toString();
    }

    public String getTopicDeleteRequest() {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("requestType", String.valueOf(RequestType.TOPIC_DELETE));
        jsonObject.addProperty("topicName", properties.getProperty("kafka.topic.name"));
        return jsonObject.toString();
    }

    public void delete() throws IOException {
        connect();
    }

    @Override
    protected void onConnect() {
        System.out.println("connected to server");
    }

    private void sendToServer() {
        try {
//            if (serverState == KafkaServerClient.ServerState.CONNECTED) write(getAuthRequest());
            if (serverState == KafkaServerClient.ServerState.CONNECTED) write(getTopicDeleteRequest());
            else if (serverState == KafkaServerClient.ServerState.AUTHENTICATED) write(getTopicDeleteRequest());
            else if (serverState == KafkaServerClient.ServerState.CLOSE) close();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            serverState = KafkaServerClient.ServerState.CLOSE;
            close();
        }
    }

    @Override
    protected void onRead(String readData) {
        final JsonObject readJson = new Gson().fromJson(readData, JsonObject.class);
        final RequestType requestType = RequestType.valueOf(readJson.get("requestType").getAsString());
        if (readJson.get("responseType").getAsString().equals("SUCCESS")) {
            if (requestType == RequestType.NEW_CONNECTION) serverState = KafkaServerClient.ServerState.CONNECTED;
            else if (requestType == RequestType.AUTH) serverState = KafkaServerClient.ServerState.AUTHENTICATED;
            else if (requestType == RequestType.TOPIC_DELETE) serverState = KafkaServerClient.ServerState.CLOSE;
        } else {
            if (requestType == RequestType.AUTH) serverState = KafkaServerClient.ServerState.CLOSE;
            else if (requestType == RequestType.TOPIC_DELETE) serverState = KafkaServerClient.ServerState.CLOSE;
            else serverState=ServerState.CLOSE;
        }
        System.out.println(readData);
        sendToServer();
    }

    @Override
    protected void onClose() {
        System.out.println("connection closed");
    }

    @Override
    protected void onSend(String message) {
        System.out.println(message);
    }

    public static void main(String[] args) throws IOException {
        Properties properties=new Properties();
        properties.setProperty("kafka.server.address","10.10.5.30");
        properties.setProperty("kafka.server.port","31491");
        properties.setProperty("kafka.topic.name","tweets");
//        properties.setProperty("kafka.topic.numberOfPartitions","2");
        final KafkaTopicDeleter kafkaTopicDeleter = new KafkaTopicDeleter(properties);
        kafkaTopicDeleter.delete();
    }
}
