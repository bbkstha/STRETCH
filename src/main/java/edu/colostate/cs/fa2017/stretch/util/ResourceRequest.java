//package edu.colostate.cs.fa2017.stretch.util;
//
//import org.apache.ignite.Ignite;
//import org.apache.ignite.IgniteMessaging;
//import org.apache.ignite.cache.CachePeekMode;
//import org.apache.ignite.cluster.ClusterGroup;
//import org.apache.ignite.cluster.ClusterMetrics;
//import org.apache.ignite.cluster.ClusterNode;
//import org.apache.ignite.configuration.IgniteConfiguration;
//import org.apache.ignite.lang.IgniteClosure;
//
//import java.util.*;
//
//public class ResourceRequest extends Thread {
//
//    private static final String REQUEST_TOPIC = "Resource_Requested";
//    private IgniteMessaging igniteMessaging;
//    private Ignite ignite;
//    private String groupName;
//    private String cacheName;
//
//    public ResourceRequest(IgniteMessaging igniteMessaging, Ignite ignite, String groupName, String cacheName){
//
//        this.igniteMessaging = igniteMessaging;
//        this.ignite = ignite;
//        this.groupName = groupName;
//        this.cacheName = cacheName;
//
//    }
//
//
//    @Override
//    public void run() {
//
//        ClusterGroup clusterGroup = ignite.cluster().forAttribute("group",groupName);
//        ClusterMetrics clusterMetrics = clusterGroup.metrics();
//
//
//            long startTimeMills = clusterMetrics.getStartTime();
//            long committedMemory = clusterMetrics.getNonHeapMemoryCommitted();
//            long usedMemory = clusterMetrics.getNonHeapMemoryUsed();
//            System.out.println("UsedMemory: "+usedMemory+" and AllocatedMemory: "+committedMemory);
//            long currentTimeMillis = System.currentTimeMillis();
//
//            double usage = usedMemory * (100 / (double) committedMemory); //usage percentage (Bytes)
//            double usageRate = usedMemory * 1000 /(currentTimeMillis - startTimeMills); //usage per second
//            double standardUsage = 476837.158203; //500MB per second
//            System.out.println("For Cluster: "+groupName);
//            System.out.println("Usage: "+usage+" and usageRage: "+usageRate);
//            if(usage > usage+1 || usageRate > usageRate+1){
//
//                Map<ClusterNode, Double> nodeToUsage = new TreeMap<>();
//                Collection<ClusterNode> nodes = clusterGroup.nodes();
//                Iterator<ClusterNode> iterator = nodes.iterator();
//                while(iterator.hasNext()){
//                    ClusterNode n = iterator.next();
//                    nodeToUsage.put(n, n.metrics().getNonHeapMemoryUsed() / (double) n.metrics().getNonHeapMemoryTotal());
//                }
//
//                ClusterNode hotspotNode = ((TreeMap<ClusterNode, Double>) nodeToUsage).lastEntry().getKey();
//                //Finding hot partition
//                int[] part = ignite.affinity(cacheName).allPartitions(hotspotNode);
//                ClusterGroup hotspotCluster = ignite.cluster().forNode(hotspotNode);
//
//                TreeMap<Integer, Long>  map = ignite.compute(hotspotCluster).apply(
//
//                        new IgniteClosure< Integer, TreeMap<Integer, Long> >() {
//                            @Override public TreeMap<Integer, Long>  apply(Integer x) {
//                                x--;
//                                TreeMap<Integer, Long> partToCount = new TreeMap<>();
//                                for(int i=0; i<part.length; i++){
//                                    Long ig = ignite.cache(cacheName).localSizeLong(part[i], CachePeekMode.PRIMARY);
//
//                                    partToCount.put(part[i], ig);
//                                }
//                                return partToCount;
//                            }
//                        },
//                        5
//                );
//
//                //calculate skewness
//
//                String hotspotPartitions = "";
//                for(int j =0; j< part.length; j=j+2){
//
//                    hotspotPartitions+= Integer.toString(part[j])+",";
//                    System.out.println("The partition is: "+part[j]+" and the count is: "+map.get(part[j]));
//                }
//
//                igniteMessaging.send(REQUEST_TOPIC, (Object) hotspotPartitions);
//            }
//            try {
//                sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//    }
//}
