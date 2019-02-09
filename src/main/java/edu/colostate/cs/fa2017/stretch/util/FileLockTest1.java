package edu.colostate.cs.fa2017.stretch.util;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

public class FileLockTest1
{
    public static void main(String[] args){

        String path = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/hashmap1.ser";
        try {
            File file = new File(path);
            FileChannel channel1 = new RandomAccessFile(file, "rws").getChannel();
            FileLock lock = channel1.lock(); //Lock the file. Block until release the lock

            System.out.println("LOCKED.");

            Map<String, Integer> map = new TreeMap<>();
            ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel1));
            map = (TreeMap<String, Integer>) ois.readObject();


            Iterator iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry mentry = (Map.Entry) iterator.next();
                System.out.print("key: " + mentry.getKey() + " & Value: ");
                System.out.println(mentry.getValue());
            }


            System.out.println("++++++++++++");

            System.out.println(map.remove("bbk"));
            System.out.println(map.remove("mm"));

            System.out.println(map.size());

            ObjectOutputStream oos = new ObjectOutputStream(Channels.newOutputStream(channel1));
            oos.writeObject(map);
            System.out.println("Waiting");
            Thread.sleep(5000);



            lock.release(); //Release the file
            System.out.println("UNLOCKED.");
            ois.close();
            oos.close();
            channel1.close();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
