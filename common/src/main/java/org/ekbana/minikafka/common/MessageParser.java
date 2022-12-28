package org.ekbana.minikafka.common;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class MessageParser {
    /**
     * contains the logic for parsing the message
     * SOH : start of header - 4 byte
     * LEN : length of message - 4 byte
     * MSG : message - N bytes
     * CRC : check sum - 4 byte
     * EOM : end of message - 4 byte
     * */
    private int length;
    private int msgOffset=0;
    private byte[] messageBytes;
    private boolean readAllBytes=false;

    public byte[] encode(String msg){
        return encode(msg.getBytes());
    }

    public byte[] encode(byte[] msgBytes){
        final byte[] lengthByte = ByteBuffer.allocate(4).putInt(msgBytes.length).array();
        byte[] totalBytes=new byte[msgBytes.length+lengthByte.length];
        for (int i=0;i<totalBytes.length;i++){
            totalBytes[i]=i<lengthByte.length?lengthByte[i]:msgBytes[i-lengthByte.length];
        }
        return totalBytes;
    }
    public boolean hasReadAllBytes(){
        return readAllBytes;
    }

    public int remainingBytes(){
        return length-msgOffset;
    }

    public String decodedMessage(){
        return new String(messageBytes);
    }

    public byte[] messageBytes(){
        return messageBytes;
    }

    public void parse(byte[] bytes){
        int offset=0;
        if (length==0){
            byte[] lengthByes=new byte[]{bytes[0],bytes[1],bytes[2],bytes[3]};
            length = ByteBuffer.wrap(lengthByes).getInt();
            System.out.println(length);
            messageBytes=new byte[length];
            offset=4;
        }

        for (int i=offset;i<bytes.length;i++){
            messageBytes[msgOffset]=bytes[i];
            msgOffset++;
            if (msgOffset==length){
                readAllBytes=true;
                break;
            }
        }
    }

    public String decode(byte[] bytes){
        byte[] lengthByes=new byte[]{bytes[0],bytes[1],bytes[2],bytes[3]};
        final int length = ByteBuffer.wrap(lengthByes).getInt();
        System.out.println(length);
        byte[] messageBytes=new byte[length];
        for (int i=4;i<bytes.length;i++){
            messageBytes[i-4]=bytes[i];
        }
        final String s = new String(messageBytes);
        return s;
    }

}
