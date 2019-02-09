package edu.colostate.cs.fa2017.stretch.util;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionX;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TestNodeStarter {

    private static final String configTemplate = "/s/chopin/b/grad/bbkstha/IdeaProjects/IgniteExamples/config/group/X/ClusterWorkerTemplate.xml";

    public static void main(String[] args){

        //IgniteConfiguration cfg = new IgniteConfiguration();
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName("TEST");
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunctionX stretchAffinityFunctionX = new StretchAffinityFunctionX(false, 10);
        cacheConfiguration.setAffinity(stretchAffinityFunctionX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.SYNC);

        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName("TEST");
        // Setting the size of the default memory region to 80MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(200L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);
        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        igniteConfiguration.setRebalanceThreadPoolSize(4);




        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group","GG");
            put("role", "master");
            put("donated","no");
            put("region-max", "100");
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        //cfg.setClientMode(true);
        Ignite ignite = Ignition.start(igniteConfiguration);

        System.out.println("FInal stage of borrowing.");

        String idleNodeTobeUsed = ignite.cluster().localNode().id().toString();
        String hotPartitions = "5,";
        String host = "cut-bank";
        String group = "GG";
        String placeHolder = "_GROUP-NAME_" + "##" + "_DONATED_" + "##" + "_HOT-PARTITIONS_"+ "##" + "_CAUSE_" +"##"+ "_IDLE-NODE_";
        String replacement = group + "##" + "yes" + "##"+hotPartitions +"##"+ "M" +"##"+ idleNodeTobeUsed;
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
