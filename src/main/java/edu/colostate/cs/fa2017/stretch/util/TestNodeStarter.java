package edu.colostate.cs.fa2017.stretch.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestNodeStarter {

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusteWorkerTemplate.xml";

    public static void main(String[] args){

        IgniteConfiguration cfg = new IgniteConfiguration();

        //cfg.setClientMode(true);
        Ignite ignite = Ignition.start(cfg);

        System.out.println("FInal stage of borrowing.");

        String hotPartitions = "511,";
        String host = "192.168.122.1";
        String group = "Z";
        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_";
        String replacement = group + "##" + "yes" + "##"+ hotPartitions;
        FileEditor fileEditor = new FileEditor(configTemplate, placeHolder, replacement, group);
        String configPath = fileEditor.replace();


        Collection<Map<String, Object>> hostNames = new ArrayList<>();
        Map<String, Object> tmpMap = new HashMap<String, Object>() {{
            put("host", host);
            put("uname", "bbkstha");
            put("passwd", "Bibek2753");
            put("cfg", configPath);
            put("nodes", 2);
        }};
        hostNames.add(tmpMap);
        Map<String, Object> dflts = null;

        System.out.println("Starting new node!!!!!");
        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);


    }
}
