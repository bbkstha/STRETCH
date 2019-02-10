package edu.colostate.cs.fa2017.stretch.util;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TestPartitionSpliter {

    public static void main(String[] args){


        char[] base32 = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
                'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        Map<String, Integer> keyToPartitionMap = new HashMap<>();
        String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-Y.ser";
        File file = new File(path);
        String skewedPartitions = "330,";
        String[] skewedPart = skewedPartitions.split(",");
        String skewedKeys = "";
        try {
            FileChannel channel1 = new RandomAccessFile(file, "rw").getChannel();
            FileLock lock = channel1.lock(); //Lock the file. Block until release the lock
            System.out.println("FILE LOCKED.");
            Map<String, Integer> map = new HashMap<>();
            ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel1));
            map = (HashMap<String, Integer>) ois.readObject();
            lock.release(); //Release the file
            System.out.println("UNLOCKED.");
            ois.close();
            channel1.close();
            keyToPartitionMap = map;

            System.out.println(skewedPart.length);
            System.out.println(skewedPart[0]);



            for (int index = 0; index < skewedPart.length; index++) {
                for (Iterator iter = keyToPartitionMap.entrySet().iterator(); iter.hasNext(); ) {
                    Map.Entry e = (Map.Entry) iter.next();
                    if (skewedPart[index].equals(e.getValue().toString())) {
                        skewedKeys += e.getKey() + ",";
                        System.out.println("Skewed key are: " + skewedKeys);
                    }
                }
            }

            System.out.println(skewedKeys);


            String[] eachSkewedKeys = skewedKeys.split(",");
            for (int index = 0; index < eachSkewedKeys.length; index++) {
                int largestPartitionID = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
                String hotKey = eachSkewedKeys[index];

                for (int j = 0; j < base32.length; j++) {
                    String tmpHotKey = hotKey + base32[j];
                    for(int k =0; k< base32.length; k++){
                        String innerTmpHotKey = tmpHotKey + base32[k];
                        keyToPartitionMap.put(innerTmpHotKey, largestPartitionID + ((32*j)+k));
                    }
                }
                keyToPartitionMap.remove(eachSkewedKeys[index]);
            }

            Iterator iterator = keyToPartitionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry mentry = (Map.Entry) iterator.next();
                System.out.print("key: " + mentry.getKey() + " & Value: ");
                System.out.println(mentry.getValue());
            }

            //Save to modified KeyToPartitionMap
            /*FileChannel channel2 = new RandomAccessFile(file, "rw").getChannel();
            FileLock lock2 = channel2.lock(); //Lock the file. Block until release the lock
            System.out.println("FILE LOCKED AGAIN.");
            ObjectOutputStream oos = new ObjectOutputStream(Channels.newOutputStream(channel2));
            oos.writeObject(keyToPartitionMap);
            lock2.release(); //Release the file
            System.out.println("UNLOCKED.");
            oos.close();
            channel2.close();*/
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

}
