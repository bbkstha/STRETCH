package edu.colostate.cs.fa2017.stretch.util;

import java.io.*;


public class FileEditor {

    String fileName;
    String placeHolder;
    String replacement;

    public FileEditor(String fileName, String placeHolder, String replacement){

        this.fileName = fileName;
        this.placeHolder = placeHolder;
        this.replacement = replacement;
    }

    private void replace() {

        String oldFileName = fileName;  //"./config/util/test.xml";
        String tmpFileName = fileName+"tmp"; //"./config/util/test1.xml";

        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(oldFileName));
            bw = new BufferedWriter(new FileWriter(tmpFileName));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains(placeHolder))
                    line = line.replace(placeHolder, replacement);
                bw.write(line+"\n");
            }
        } catch (Exception e) {
            return;
        } finally {
            try {
                if(br != null)
                    br.close();
            } catch (IOException e) {
                //
            }
            try {
                if(bw != null)
                    bw.close();
            } catch (IOException e) {
                //
            }
        }
        // Once everything is complete, delete old file..
        File oldFile = new File(oldFileName);
        oldFile.delete();

        // And rename tmp file's name to old file name
        File newFile = new File(tmpFileName);
        newFile.renameTo(oldFile);

    }
}
