package edu.colostate.cs.fa2017.stretch.util;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import scala.reflect.internal.Trees;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

public class FileLockTest {

    private static final char[] base32 = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    private static Map<String, Integer> keyToPartitionMap = new HashMap<>();

    public static void main(String[] args) {

        String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-Y.ser";
        try {

            File file = new File(path);
            FileChannel fileChannel = new RandomAccessFile(file, "rws").getChannel();
            FileLock fileLock = fileChannel.lock();

            for (int i = 0; i < base32.length; i++) {
                for (int j = 0; j < base32.length; j++) {
                    String tmp = Character.toString(base32[i]);
                    tmp += Character.toString(base32[j]);
                    keyToPartitionMap.put(tmp, (32 * i) + j);
                }
            }
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(Channels.newOutputStream(fileChannel));
            objectOutputStream.writeObject(keyToPartitionMap);
            fileLock.release();
            objectOutputStream.close();
            fileChannel.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            File file = new File(path);
            FileChannel fileChannel = new RandomAccessFile(file, "rws").getChannel();
            FileLock fileLock = fileChannel.lock();
            Map<String, Integer> map = new TreeMap<>();
            ObjectInputStream objectInputStream = new ObjectInputStream(Channels.newInputStream(fileChannel));
            map = (HashMap<String, Integer>) objectInputStream.readObject();

            Iterator iterator = map.entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry mentry = (Map.Entry)iterator.next();
                System.out.print("key: "+ mentry.getKey() + " & Value: ");
                System.out.println(mentry.getValue());
            }

            fileLock.release();
            objectInputStream.close();
            fileChannel.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
