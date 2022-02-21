package org.ekbana.server.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Serializer {

    public byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream outputStream=new ByteArrayOutputStream();
        ObjectOutputStream os=new ObjectOutputStream(outputStream);
        os.writeObject(obj);
        return outputStream.toByteArray();
    }
}
