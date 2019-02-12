package edu.colostate.cs.fa2017.stretch.util;

import java.io.*;
import java.util.*;

public class GenerateMutipleLevelPrecision {
    private static final char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

    private static Map<String, Integer > keyToPartitionMap = new HashMap<>();


    public static void main(String[] args){

        Map<String, Integer> map = new HashMap<>();
        try
        {
            FileInputStream fis = new FileInputStream("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-X.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap<String, Integer>) ois.readObject();
            ois.close();
            fis.close();
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
            return;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Class not found");
            c.printStackTrace();
            return;
        }
        keyToPartitionMap = map;


        String skewedKeys = "bb,";

        String[] eachSkewedKeys = skewedKeys.split(",");
        for (int index = 0; index < eachSkewedKeys.length; index++) {
            int largestPartitionID = Collections.max(keyToPartitionMap.entrySet(), Map.Entry.comparingByValue()).getValue() + 1;
            String hotKey = eachSkewedKeys[index];
            System.out.println("The hotkey is: "+hotKey);
            for (int j = 0; j < base32.length; j++) {
                String tmpHotKey = hotKey + base32[j];
                for(int k =0; k< base32.length; k++){
                    String innerTmpHotKey = tmpHotKey + base32[k];
                    keyToPartitionMap.put(innerTmpHotKey, largestPartitionID + ((32*j)+k));
                    //System.out.println("The inner hotKey is: "+innerTmpHotKey+" and partID is: "+keyToPartitionMap.get(innerTmpHotKey));
                }
            }
            keyToPartitionMap.remove(eachSkewedKeys[index]);
        }

        try
        {
            FileOutputStream fos =
                    new FileOutputStream("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-X.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(keyToPartitionMap);
            oos.close();
            fos.close();
            System.out.printf("Serialized HashMap data is saved in hashmap.ser");
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
        }


        try
        {
            FileInputStream fis = new FileInputStream("/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-X.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap<String, Integer>) ois.readObject();
            ois.close();
            fis.close();
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
            return;
        }catch(ClassNotFoundException c)
        {
            System.out.println("Class not found");
            c.printStackTrace();
            return;
        }

        Set set = map.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry)iterator.next();
            System.out.print("key: "+ mentry.getKey() + " & Value: ");
            System.out.println(mentry.getValue());
        }


    }
}
