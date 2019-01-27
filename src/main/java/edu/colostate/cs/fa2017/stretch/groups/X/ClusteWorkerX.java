package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.util.FileEditor;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class ClusteWorkerX {

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusteWorkerTemplate.xml";

    public static void main(String[] args){

        if(args.length<1){
            return;
        }
        String group = args[0];

        String placeHolder = "_GROUP-NAME_";
        String replacement = group;

        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
        String configPath = fileEditor.replace();

        System.out.println(configPath);


        Ignite ignite = Ignition.start(configPath);
    }



}
