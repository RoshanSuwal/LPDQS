package org.ekbana.broker.storage.file;

import org.apache.commons.io.FileUtils;

import java.io.*;

public class FileWriter {
    // writes records to file in append mode

    private final File file;
    BufferedWriter bufferedWriter;

    public FileWriter(String fileName){
        file=new File(fileName);
    }

    public void open() throws IOException {
        bufferedWriter=new BufferedWriter(new java.io.FileWriter(file));
        FileOutputStream fileOutputStream=new FileOutputStream(file);
        ObjectOutputStream objectOutputStream=new ObjectOutputStream(fileOutputStream);
//        FileUtils.writeByteArrayToFile();
    }

    public long write(byte[] bytes){
        return 0;
    }

    public void close() throws IOException {
        if (bufferedWriter!=null) bufferedWriter.close();
    }
}
