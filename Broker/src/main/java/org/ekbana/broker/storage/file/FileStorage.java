package org.ekbana.broker.storage.file;

import org.ekbana.broker.record.Record;
import org.ekbana.broker.storage.Storage;

import java.io.*;

public class FileStorage<T extends Record> implements Storage<T> {
    private final String rootPath;
    private long size;
    private long count;

    public FileStorage(String rootPath) {
        this.rootPath = rootPath;
        this.size=0;
        this.count=0;
    }

    @Override
    public void store(T t) {
        System.out.println(t.toString());
        try {
            writeRecordToFile(t);
//            createAndWriteIntoFile(t.getOffset(), t.getData().getBytes());
            this.size = this.size + t.size();
            count = count + 1;
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public T get(long position) {
        try {
            return (T) readRecordFromFile((int) position);
        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
            System.out.println(e.getMessage());
            return null;
        }
    }

    private Object readRecordFromFile(int position) throws IOException, ClassNotFoundException {
            FileInputStream fileInputStream=new FileInputStream(rootPath+position+".dat");
            ObjectInputStream objectInputStream=new ObjectInputStream(fileInputStream);
        final Object o = objectInputStream.readObject();
        objectInputStream.close();
        fileInputStream.close();
        return o;
    }

    private void writeRecordToFile(T t) throws IOException {
        FileOutputStream fos=new FileOutputStream(rootPath+t.getOffset()+".dat");
        ObjectOutputStream oos=new ObjectOutputStream(fos);
        oos.writeObject(t);
        oos.close();
        fos.close();
    }

    private void createAndWriteIntoFile(long fileId,byte[] bytes) throws IOException {
        File file=new File(rootPath+fileId+".log");
        FileOutputStream fileOutputStream=new FileOutputStream(file,false);
        fileOutputStream.write(bytes);
        fileOutputStream.flush();
        fileOutputStream.close();
    }

    public static void main(String[] args) {
        FileStorage<Record> fileStorage=new FileStorage<>("data/");

        Record record=new Record("hello world");
        record.setTopic("test-1");
        record.setPartitionId(1);
        record.setOffset(2);

        fileStorage.store(record);
        record.setOffset(3);
        fileStorage.store(record);

        final Record record1 = fileStorage.get(6);
        System.out.println(record1);

    }
}
