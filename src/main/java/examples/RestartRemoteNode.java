package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.*;


public class RestartRemoteNode {


    public static void main(String[] args){

        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignite ignite = Ignition.start(cfg);


        System.out.println("The size was: "+ignite.cluster().nodes().size());
        ClusterNode remoteNode = ignite.cluster().forRemotes().node();


        String host = "cut-bank"; //remoteNode.hostNames().iterator().next();
        System.out.println("The remote hostname was: "+host);
        Collection<Map<String, Object>> hostNames = new ArrayList<>();
        Map<String, Object> tmpMap = new HashMap<String, Object>(){{
                put("host",host);
                put("uname","bbkstha");
                put("passwd","Bibek2753");
                put("cfg","/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/A/GroupA.xml");
                put("nodes",3);
            }};
        hostNames.add(tmpMap);
        //UUID id = remoteNode.id();
        //System.out.println("The remote node id is: " + id);


        Map<String, Object> dflts = null;
//        dflts.put("uname","bbkstha");
//        dflts.put("passwd","Bibek2753");
//        dflts.put("cfg","/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/A/GroupA.xml");
//        //ignite.cluster().stopNodes(Collections.singleton(id));

        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //System.out.println("Starting again.");
        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);
        //ignite.cluster().restartNodes(Collections.singleton(remoteNode.id()));
        System.out.println("The size is: "+ignite.cluster().nodes().size());
    }


}
