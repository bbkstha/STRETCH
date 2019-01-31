package edu.colostate.cs.fa2017.stretch.util;


import java.util.*;

import static org.apache.ignite.internal.util.lang.GridFunc.rand;

public class SortMapUsingValue {


    public static void main(String[] args) {
        System.out.println("\nSorting using Java8 streams\n");

        System.out.println((double)(Math.random() % 0.00001));
        System.out.println();

        String a = " 726,601,527,599,670,720,23,951, ";
        String[] b = a.split(",");

        System.out.println("The length of b is: "+b.length);
        int[] c = new int[b.length];

        for (int k = 0; k < b.length; k++) {
            c[k] = Integer.parseInt(b[k].trim());
        }

        Arrays.sort(c);
        for (int m = 0; m < c.length; m++) {
            System.out.println(c[m]);
        }



        //sortByValueJava8Stream();
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
