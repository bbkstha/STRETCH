package edu.colostate.cs.fa2017.stretch.util;

import java.io.DataOutput;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SmallTestClass {

    public static void main(String[] args){

       /* int x = (int) Math.ceil(1024 / (double) 5);
        System.out.println(x);*/

       /* String d = "-77.5590057373046900";
        System.out.println(d.substring(0, d.indexOf(".")));*/
        /*List<File> list = listf("/s/chopin/b/grad/bbkstha/stretch/data");
        System.out.println(list.size());

        for(File f: list){
            System.out.println(f.getPath());
        }*/

        double cpu= 0.0033333333333333335;
        long c = (long) (cpu*1000000000);
        double pc = c / 1000000000.0;
        System.out.println(c);
        System.out.println(pc);

    }

    public static List<File> listf(String directoryName) {
        File directory = new File(directoryName);

        List<File> resultList = new ArrayList<File>();

        // get all the files from a directory
        File[] fList = directory.listFiles();
        resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
                //System.out.println(file.getAbsolutePath());
            } else if (file.isDirectory()) {
                resultList.remove(file);
                resultList.addAll(listf(file.getAbsolutePath()));
            }
        }
        //System.out.println(fList);
        return resultList;
    }
}
