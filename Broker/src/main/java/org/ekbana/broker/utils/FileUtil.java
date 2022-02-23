package org.ekbana.broker.utils;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtil {
    /*get the list of all the files present in given folder*/
    public static Stream<File> getDirectories(String directory) throws IOException {
        return Stream.of(Objects.requireNonNull(new File(directory).listFiles())).filter(File::isDirectory);
//        Path path=Path.of(directory);
//        return Files.walk(path,1);
    }

    public static Stream<File> getFiles(String directory) throws IOException {
        return Stream.of(Objects.requireNonNull(new File(directory).listFiles())).filter(File::isFile);
//        Path path=Path.of(directory);
//        return Files.walk(path,1);
    }


    /* get the file name with max file name*/
    public long getFolderSize(String directory){
        return 0;
    }
    /* get the size of the folder*/
    /* get the first and last document of file*/
    /* write object to given file*/
    public static void writeObjectToFile(String path,Object object) throws IOException {
        FileOutputStream fos=new FileOutputStream(path);
        ObjectOutputStream oos=new ObjectOutputStream(fos);
        oos.writeObject(object);
        oos.flush();
        oos.close();
        fos.close();
    }

    public static Object readObjectFromFile(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream=new FileInputStream(path);
        ObjectInputStream objectInputStream=new ObjectInputStream(fileInputStream);
        final Object o = objectInputStream.readObject();
        objectInputStream.close();
        fileInputStream.close();
        return o;
    }

    public static void deleteDirectory(String directory){
        try {
            FileUtils.deleteDirectory(new File(directory));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteFile(String file){
        try {
            FileUtils.delete(new File(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeStreamToFile(String path, List<?> list,Type type) throws IOException {
//        System.out.println("Stream to file");
//        final FileWriter out = new FileWriter(path, false);
//        PrintWriter printWriter=new PrintWriter(out);
//        list
////                .peek(System.out::println)
//                .forEach(object->{printWriter.println(object);printWriter.flush();});
//        printWriter.close();
//        out.close();
        final Gson gson = new Gson();
        FileUtils.writeLines(new File(path),list.stream().map(gson::toJson).collect(Collectors.toList()));
    }

    public static List<?> readAllLines(String path, Type type) throws IOException {
//        BufferedReader bufferedReader=new BufferedReader(new FileReader(path));
//        List<String> list=new ArrayList<>();
//        String line;
//        while ((line=bufferedReader.readLine())!=null){
//            list.add(line);
//        }
        final Gson gson = new Gson();

        return FileUtils.readLines(new File(path), Charset.defaultCharset())
                .stream()
                .map(line->gson.fromJson(line,type))
                .collect(Collectors.toList());
    }
}
