package edu.colostate.cs.fa2017.stretch.groups.X;

import edu.colostate.cs.fa2017.stretch.affinity.StretchAffinityFunctionXX;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.HashMap;
import java.util.Map;

public class ClusterWorkerX {

    private static final String cacheName = "STRETCH-CACHE";
    private static final String dataRegionName = "150MB_Region";

    public static void main(String[] args){

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(cacheName);
        cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);

        StretchAffinityFunctionXX stretchAffinityFunctionXX = new StretchAffinityFunctionXX(false, 6400);
        cacheConfiguration.setAffinity(stretchAffinityFunctionXX);
        cacheConfiguration.setRebalanceMode(CacheRebalanceMode.SYNC);




        // Changing total RAM size to be used by Ignite Node.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        // Region name.
        regionCfg.setName(dataRegionName);
        // Setting the size of the default memory region tevent.equalsIgnoreCase("NODE-JOINED")o 100MB to achieve this.
        regionCfg.setInitialSize(
                50L * 1024 * 1024);
        regionCfg.setMaxSize(300L * 1024 * 1024);
        // Enable persistence for the region.
        regionCfg.setPersistenceEnabled(false);

        storageCfg.setSystemRegionMaxSize(45L * 1024 * 1024);
        // Setting the data region configuration.
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        // Applying the new configuration.
        igniteConfiguration.setDataStorageConfiguration(storageCfg);
        igniteConfiguration.setRebalanceThreadPoolSize(4);
        Map<String, String> userAtt = new HashMap<String, String>() {{
            put("group","X");
            put("role", "worker");
            put("donated","no");
            put("region-max", "300");
            put("split","no");
        }};
        igniteConfiguration.setCacheConfiguration(cacheConfiguration);
        igniteConfiguration.setUserAttributes(userAtt);
        igniteConfiguration.setClientMode(false);

        // Start Ignite node.
        Ignite ignite = Ignition.start(igniteConfiguration);

            //ClusterGroup masterGroup = ignite.cluster().forAttribute("role", "master");

            //IgniteMessaging mastersMessanger = ignite.message(masterGroup);
            //Map<UUID, Object> offerReceived = new HashMap<>();




/*            //All other listeners here!!

            //4.Listen for offer grant
            mastersMessanger.remoteListen(OFFER_GRANTED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (msg.toString().split("::")[0].equals(ignite.cluster().localNode().id())) {

                        String[] param = msg.toString().split("::");
                        String hotPartitions = param[1];
                        String host = param[2];
                        String group = ignite.cluster().localNode().attribute("group");
                        String placeHolder = "_GROUP-NAME_" + "$$" + "_DONATED_" + "$$" + "_HOT-PARTITIONS_";
                        String replacement = group + "##" + "yes" + hotPartitions;
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

                        ignite.cluster().startNodes(hostNames, dflts, false, 10000, 5);
                    }
                    return true;
                }
            });


            //3. Listen for acknowledgement
            mastersMessanger.remoteListen(OFFER_ACKNOWLEDGED, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (msg.toString().split("::")[0].equals(ignite.cluster().localNode().id())) {
                        //Transfer data or persist it in the disk from the donated node
                        //Send the final permission
                        Object reply = nodeId + "::" + msg.toString().split("::")[1] + "::" + msg.toString().split("::")[2];
                        mastersMessanger.send(OFFER_GRANTED, reply);
                    }
                    return true;
                }
            });


            //2. Listen for resource offered.
            //Listen to service message. Expect receiving node that has been donated.
            mastersMessanger.remoteListen(OFFER_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {
                    if (!nodeId.equals(ignite.cluster().localNode().id())) {
                        int expectedNumberOfOffers = ignite.cluster().forAttribute("role", "master").nodes().size() - 1;
                        offerReceived.put(nodeId, msg);
                        if (offerReceived.size() < expectedNumberOfOffers) {
                            return true;
                        }

                        String hotPartitions = msg.toString().split("::")[2];
                        //Make selection of hostname to use
                        double usage = 1.0;
                        String host = "";
                        UUID sender = null;
                        for (Map.Entry<UUID, Object> e : offerReceived.entrySet()) {

                            String[] offer = e.getValue().toString().split("::");

                            if (Double.parseDouble(offer[0]) < usage) {
                                usage = Double.parseDouble(offer[0]);
                                host = offer[1];
                                sender = e.getKey();
                            }
                        }
                        Object reply = sender + "::" + hotPartitions + "::" + host;

                        mastersMessanger.send(OFFER_ACKNOWLEDGED, reply);
                    }
                    return true;
                }
            });


            //1. Listen to Resource Request
            mastersMessanger.remoteListen(REQUEST_TOPIC, new IgniteBiPredicate<UUID, Object>() {
                @Override
                public boolean apply(UUID nodeId, Object msg) {

                    if (!nodeId.equals(ignite.cluster().localNode().id())) {

                        String groupName = ignite.cluster().localNode().attribute("group");
                        ClusterGroup workerCluster = ignite.cluster().forAttribute("group", groupName).forAttribute("role", "worker");
                        if (!workerCluster.nodes().isEmpty()) {
                            Map<String, Integer> nodesPerHost = new HashMap<>();
                            Map<Double, String> usagePerNode = new TreeMap<>();
                            Iterator<ClusterNode> iterator = workerCluster.nodes().iterator();
                            while (iterator.hasNext()) {
                                ClusterNode n = iterator.next();
                                String hostname = n.hostNames().iterator().next();
                                double usage = n.metrics().getNonHeapMemoryUsed() / (double) n.metrics().getNonHeapMemoryTotal();


                                if (!nodesPerHost.containsKey(hostname)) {
                                    nodesPerHost.put(hostname, 1);
                                    usagePerNode.put(usage, hostname);
                                } else {
                                    nodesPerHost.put(hostname, nodesPerHost.get(hostname) + 1);
                                }
                            }
                            //This is the host that can be used to spawn new JVM.
                            Double usage = ((TreeMap<Double, String>) usagePerNode).firstKey();
                            String idleHost = ((TreeMap<Double, String>) usagePerNode).firstEntry().getValue();
                            String hotPartitions = msg.toString();
                            Object offer = (Object) usage + "::" + idleHost + "::" + hotPartitions;
                            mastersMessanger.send(OFFER_TOPIC, offer);
                        }
                    }
                    return true;
                }
            });*/



    }
}
