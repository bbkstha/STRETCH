package edu.colostate.cs.fa2017.stretch.util;

import org.apache.ignite.Ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NodeStarter {

    private String hostName;
    private String group;
    private Ignite ignite;


    public NodeStarter(String hostName, String group, Ignite ignite){

        this.hostName = hostName;
        this.group = group;
        this.ignite = ignite;
    }

    public void start(){
        Collection<Map<String, Object>> hostNames = new ArrayList<>();
        Map<String, Object> tmpMap = new HashMap<String, Object>(){{
            put("host",hostName);
            put("uname","bbkstha");
            put("passwd","Bibek2753");
            put("cfg","/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/A/GroupGG.xml");
            put("nodes",1);
        }};
        hostNames.add(tmpMap);
        Map<String, Object> dflts = null;

        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);
    }
}

