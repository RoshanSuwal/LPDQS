package org.ekbana.broker.segment;

import org.ekbana.broker.record.Record;
import org.ekbana.broker.storage.Storage;
import org.ekbana.broker.storage.file.FileStorage;
import org.ekbana.broker.utils.BrokerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SegmentService {
    private static final ExecutorService executorService= Executors.newFixedThreadPool(3);

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
    public static Segment createActiveSegment(String topic, SegmentMetaData segmentMetaData) {
        try {
            Path rootDir = Path.of(BrokerConfig.getInstance().getDATA_PATH() + topic);
            Path segmentDir = Path.of(BrokerConfig.getInstance().getDATA_PATH() + topic + "/" + segmentMetaData.getSegmentId());
            if (Files.notExists(rootDir)) Files.createDirectory(rootDir);
            if (Files.notExists(segmentDir)) Files.createDirectory(segmentDir);
            return new Segment(segmentMetaData, new FileStorage<>(segmentDir.toString()+"/"));
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
    public static Storage<Record> getStorage(String topic, SegmentMetaData segmentMetaData){
        return new FileStorage<>(BrokerConfig.getInstance().getDATA_PATH()+topic+"/"+segmentMetaData.getSegmentId()+"/");
    }

    public static void main(String[] args) throws IOException {
        final Segment activeSegment = createActiveSegment("test-1", SegmentMetaData.builder().segmentId(10).build());
    }

}
