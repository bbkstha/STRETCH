package edu.colostate.cs.fa2017.stretch.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.*;

public class CheckSavedMap {
    public static void main(String[] args){
/*
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

        Set set = map.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry)iterator.next();
            System.out.print("key: "+ mentry.getKey() + " & Value: ");
            System.out.println(mentry.getValue());
        }
        */

        int[] current = {5,6};
        Map<String, Boolean> tmpMap = new HashMap<>();
        String b = "1,2,3,4,";
        String[] m = b.split(",");
        for(int i=0; i< m.length; i++){
            tmpMap.put(m[i],true);
        }

        System.out.println(tmpMap.size());
        boolean flag = true;
        while(flag){
            for(int j = 0; j<current.length; j++){
                if(tmpMap.containsKey(Integer.toString(current[j]))){
                    break;
                }
                flag = false;
            }
            System.out.println("in the loop");
        }

        System.out.println("out of the loop");





    }
}
