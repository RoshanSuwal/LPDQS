package org.ekbana.broker.segment;

import org.ekbana.broker.storage.Storage;
import org.ekbana.broker.storage.file.FileStorage;
import org.ekbana.broker.utils.KafkaBrokerProperties;
import org.ekbana.minikafka.common.Record;
import org.ekbana.minikafka.common.SegmentMetaData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentService {
    private static final ExecutorService executorService= Executors.newFixedThreadPool(3);

    private static final KafkaBrokerProperties brokerConfig=new KafkaBrokerProperties(new Properties());

    public static void registerTask(SegmentTask segmentTask){
        executorService.execute(segmentTask);
    }

    public static void terminate(){
        executorService.shutdown();
    }

    /**
     * creates active segment
     * @param topic             name of topic
     * @param segmentMetaData   information of segment
     * */
    public static Segment createActiveSegment(KafkaBrokerProperties brokerProperties,String topic, SegmentMetaData segmentMetaData) {
        try {
            Path rootDir = Path.of(brokerProperties.getRootPath()+brokerProperties.getDataPath() + topic);
            Path segmentDir = Path.of(brokerProperties.getRootPath()+brokerProperties.getDataPath() + topic + "/" + segmentMetaData.getSegmentId());
            if (Files.notExists(rootDir)) Files.createDirectory(rootDir);
            if (Files.notExists(segmentDir)) Files.createDirectory(segmentDir);
            return new Segment(segmentMetaData, new FileStorage(segmentDir.toString()+"/"));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * get storage
     * @param topic             name of topic
     * @param segmentMetaData   information about segment
     * @return storage for given segment of the topic
     * */
    public static Storage<Record> getStorage(KafkaBrokerProperties brokerProperties,String topic, SegmentMetaData segmentMetaData){
        return new FileStorage<>(brokerProperties.getRootPath()+brokerProperties.getDataPath()+topic+"/"+segmentMetaData.getSegmentId()+"/");
    }

    public static void main(String[] args) throws IOException {
        KafkaBrokerProperties brokerProperties=new KafkaBrokerProperties(new Properties());
        final Segment activeSegment = createActiveSegment(brokerProperties,"test-1", SegmentMetaData.builder().segmentId(10).build());
    }

}
