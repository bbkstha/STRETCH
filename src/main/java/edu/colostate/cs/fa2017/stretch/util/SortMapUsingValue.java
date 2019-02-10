package edu.colostate.cs.fa2017.stretch.util;


import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import scala.reflect.internal.Trees;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.util.lang.GridFunc.rand;

public class SortMapUsingValue {

    private static final char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

    private static Map<String, Integer > keyToPartitionMap = new HashMap<>();


    public static void main(String[] args) {
//        System.out.println("\nSorting using Java8 streams\n");
//
//        System.out.println((double)(Math.random() % 0.00001));
//        System.out.println();
//
//        String a = " 726,601,527,599,670,720,23,951, ";
//        String[] b = a.split(",");
//
//        System.out.println("The length of b is: "+b.length);
//        int[] c = new int[b.length];
//
//        for (int k = 0; k < b.length; k++) {
//            c[k] = Integer.parseInt(b[k].trim());
//        }
//
//        Arrays.sort(c);
//        for (int m = 0; m < c.length; m++) {
//            System.out.println(c[m]);
//        }

        //System.out.println("The answer is: "+(4&3));


        /*String partitionToMove = "2,3,4,5,";
        System.out.println(partitionToMove);
        System.out.println("The length of partitions to move is: " + partitionToMove.split(",").length);
        System.out.println("The length of partitions to move is: " + partitionToMove.substring(0, partitionToMove.length()-1).split(",").length);
*/

        for(int i=0; i< base32.length; i++){
            for(int j = 0; j< base32.length; j++){
                String tmp = Character.toString(base32[i]);
                tmp+=Character.toString(base32[j]);
                keyToPartitionMap.put(tmp,(32*i)+j);
            }
        }
       /* for(int j = 0; j< base32.length; j++){

            String tmpHotKey = "bb";

            tmpHotKey+=Character.toString(base32[j]);
            keyToPartitionMap.put(tmpHotKey,(1024+j));
        }
        keyToPartitionMap.remove("bb");*/

       // System.out.println(keyToPartitionMap.size());


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


        /*for(int j = 0; j< base32.length; j++){

            String tmpHotKey = "bbk";

            tmpHotKey+=Character.toString(base32[j]);
            map.put(tmpHotKey,(map.size()+j));
        }
        map.remove("bbk");

        try {
            FileOutputStream fos =
                    new FileOutputStream("./hashmap1.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            oos.close();
            fos.close();
            System.out.printf("Serialized HashMap data is saved in hashmap1.ser");
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
        }

        try
        {
            FileInputStream fis = new FileInputStream("./hashmap1.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap) ois.readObject();
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
        }*/
      /*  System.out.println(((TreeMap<String, Integer>) map).lastEntry());
        System.out.println(Collections.max(map.entrySet(), Map.Entry.comparingByValue()).getValue());


        map.put("bbk", 1025);




        System.out.println(((TreeMap<String, Integer>) map).lastEntry());
        System.out.println(Collections.max(map.entrySet(), Map.Entry.comparingByValue()).getValue());
*/









        // sortByValueJava8Stream();
    }

    private static void sortByValueJava8Stream()
    {
        Map<String, Integer> unSortedMap = getUnSortedMap();

        System.out.println("Unsorted Map : " + unSortedMap);

        LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
        unSortedMap.entrySet().stream().sorted(Map.Entry.comparingByValue())
                .forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));

        System.out.println("Sorted Map   : " + sortedMap);

        LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>();
        unSortedMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

        System.out.println("Reverse Sorted Map   : " + reverseSortedMap);

        for(Map.Entry<String, Integer> e: reverseSortedMap.entrySet()){

            System.out.println(e);
        }

        System.out.println("Again.");
        for(Map.Entry<String, Integer> e: reverseSortedMap.entrySet()){

            System.out.println(e);
        }
    }

    private static Map<String, Integer> getUnSortedMap()
    {
        Map<String, Integer> unsortMap = new HashMap<>();
        unsortMap.put("alex", 1);
        unsortMap.put("david", 2);
        unsortMap.put("elle", 3);
        unsortMap.put("charles", 4);
        unsortMap.put("brian", 4);
        return unsortMap;
    }



}
