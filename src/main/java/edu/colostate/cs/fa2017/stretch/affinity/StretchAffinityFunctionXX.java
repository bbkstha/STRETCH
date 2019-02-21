package edu.colostate.cs.fa2017.stretch.affinity;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

/**
 * Affinity function for partitioned cache based on Highest Random Weight algorithm.
 * This function supports the following configuration:
 * <ul>
 * <li>
 *      {@code partitions} - Number of partitions to spread across nodes.
 * </li>
 * <li>
 *      {@code excludeNeighbors} - If set to {@code true}, will exclude same-host-neighbors
 *      from being backups of each other. This flag can be ignored in cases when topology has no enough nodes
 *      for assign backups.
 *      Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
 * </li>
 * <li>
 *      {@code backupFilter} - Optional filter for back up nodes. If provided, then only
 *      nodes that pass this filter will be selected as backup nodes. If not provided, then
 *      primary and backup nodes will be selected out of all nodes available for this cache.
 * </li>
 * </ul>
 * <p>
 * Cache affinity can be configured for individual caches via {@link CacheConfiguration#getAffinity()} method.
 */
public class StretchAffinityFunctionXX implements AffinityFunction, Serializable {

    //Added bbkstha
    private static volatile Map<String, Integer> keyToPartitionMap = new HashMap<>();
    private int precision = 0; //Hardcoded for testing
    private static final char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };



    @IgniteInstanceResource
    private Ignite ignite;





    /***********************************/



    /** */
    private static final long serialVersionUID = 0L;

    /** Default number of partitions. */
    public static final int DFLT_PARTITION_COUNT = 10;

    /** Comparator. */
    private static final Comparator<IgniteBiTuple<Long, ClusterNode>> COMPARATOR = new HashComparator();

    /** Number of partitions. */
    private int parts = 1024+32;

    /** Mask to use in calculation when partitions count is power of 2. */
    private int mask = -1;

    /** Exclude neighbors flag. */
    private boolean exclNeighbors;

    /** Exclude neighbors warning. */
    private transient boolean exclNeighborsWarn;

    /** Optional backup filter. First node is primary, second node is a node being tested. */
    private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

    /** Optional affinity backups filter. The first node is a node being tested,
     *  the second is a list of nodes that are already assigned for a given partition (the first node in the list
     *  is primary). */
    private IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter;

    /** Logger instance. */
    @LoggerResource
    private transient IgniteLogger log;

    /**
     * Empty constructor with all defaults.
     */
    public StretchAffinityFunctionXX() {
        this(false);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
     * and specified number of backups.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     */
    public StretchAffinityFunctionXX(boolean exclNeighbors) {
        this(exclNeighbors, DFLT_PARTITION_COUNT);
    }

    /**
     * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
     * and specified number of backups and partitions.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
     *      of each other.
     * @param parts Total number of partitions.
     */
    public StretchAffinityFunctionXX(boolean exclNeighbors, int parts) {
        this(exclNeighbors, parts, null);
    }

    /**
     * Initializes optional counts for replicas and backups.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param parts Total number of partitions.
     * @param backupFilter Optional back up filter for nodes. If provided, backups will be selected
     *      from all nodes that pass this filter. First argument for this filter is primary node, and second
     *      argument is node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     */
    public StretchAffinityFunctionXX(int parts, @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this(false, parts, backupFilter);
    }

    /**
     * Private constructor.
     *
     * @param exclNeighbors Exclude neighbors flag.
     * @param parts Partitions count.
     * @param backupFilter Backup filter.
     */
    private StretchAffinityFunctionXX(boolean exclNeighbors, int parts,
                                      IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        A.ensure(parts > 0, "parts > 0");

        this.exclNeighbors = exclNeighbors;

        setPartitions(parts);

        this.backupFilter = backupFilter;

        initializeKeyToPartitionMap();

    }

    /********************************************************/


    private void initializeKeyToPartitionMap(){


       // String path = "/s/chopin/b/grad/bbkstha/Softwares/apache-ignite-2.7.0-bin/STRETCH/KeyToPartitionMap-X.ser";
        keyToPartitionMap = new HashMap<>();
        for(int i=0; i< base32.length; i++){
            for(int j = 0; j< base32.length; j++){
                String tmp = Character.toString(base32[i]);
                tmp+=Character.toString(base32[j]);
                keyToPartitionMap.put(tmp,(32*i)+j);
            }
        }
        /*try
        {
            FileOutputStream fos =
                    new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(keyToPartitionMap);
            oos.close();
            fos.close();
            System.out.printf("Serialized HashMap data is saved in hashmap.ser");
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
        }

        Map<String, Integer> map = new HashMap<>();

        try {
            FileChannel channel1 = new RandomAccessFile(path, "rw").getChannel();
            FileLock lock = channel1.lock(); //Lock the file. Block until release the lock
            System.out.println("LOCKED IN AF.");
            ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel1));
            map = (HashMap<String, Integer>) ois.readObject();
            System.out.println("UNLOCKED IN AF.");
            keyToPartitionMap = map;
            lock.release();
            ois.close();
            channel1.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        Set set = keyToPartitionMap.entrySet();
        Iterator iterator = set.iterator();
        while(iterator.hasNext()) {
            Map.Entry mentry = (Map.Entry)iterator.next();
            System.out.print("key: "+ mentry.getKey() + " & Value: ");
            System.out.println(mentry.getValue());
        }
*/

        System.out.println("Initialization done.");
    }










    /********************************************************/


















    /**
     * Gets total number of key partitions. To ensure that all partitions are
     * equally distributed across all nodes, please make sure that this
     * number is significantly larger than a number of nodes. Also, partition
     * size should be relatively small. Try to avoid having partitions with more
     * than quarter million keys.
     * <p>
     * For fully replicated caches this method works the same way as a partitioned
     * cache.
     *
     * @return Total partition count.
     */
    public int getPartitions() {
        return parts;
    }

    /**
     * Sets total number of partitions.If the number of partitions is a power of two,
     * the PowerOfTwo hashing method will be used.  Otherwise the Standard hashing
     * method will be applied.
     *
     * @param parts Total number of partitions.
     * @return {@code this} for chaining.
     */
    public StretchAffinityFunctionXX setPartitions(int parts) {
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT,
                "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);
        A.ensure(parts > 0, "parts > 0");

        this.parts = parts;

        mask = (parts & (parts - 1)) == 0 ? parts - 1 : -1;

        return this;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, ClusterNode> getBackupFilter() {
        return backupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is primary node,
     * and second node is a node being tested.
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param backupFilter Optional backup filter.
     * @deprecated Use {@code affinityBackupFilter} instead.
     * @return {@code this} for chaining.
     */
    @Deprecated
    public StretchAffinityFunctionXX setBackupFilter(
            @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
        this.backupFilter = backupFilter;

        return this;
    }

    /**
     * Gets optional backup filter. If not {@code null}, backups will be selected
     * from all nodes that pass this filter. First node passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return Optional backup filter.
     */
    @Nullable public IgniteBiPredicate<ClusterNode, List<ClusterNode>> getAffinityBackupFilter() {
        return affinityBackupFilter;
    }

    /**
     * Sets optional backup filter. If provided, then backups will be selected from all
     * nodes that pass this filter. First node being passed to this filter is a node being tested,
     * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is the first in the list).
     * <p>
     * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     * <p>
     * //For an example filter, see {@link //ClusterNodeAttributeAffinityBackupFilter }.
     *
     * @param affinityBackupFilter Optional backup filter.
     * @return {@code this} for chaining.
     */
    public StretchAffinityFunctionXX setAffinityBackupFilter(
            @Nullable IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter) {
        this.affinityBackupFilter = affinityBackupFilter;

        return this;
    }

    /**
     * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @return {@code True} if nodes residing on the same host may not act as backups of each other.
     */
    public boolean isExcludeNeighbors() {
        return exclNeighbors;
    }

    /**
     * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
     * <p>
     * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
     *
     * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
     * @return {@code this} for chaining.
     */
    public StretchAffinityFunctionXX setExcludeNeighbors(boolean exclNeighbors) {
        this.exclNeighbors = exclNeighbors;

        return this;
    }

    /**
     * Resolves node hash.
     *
     * @param node Cluster node;
     * @return Node hash.
     */
    public Object resolveNodeHash(ClusterNode node) {
        return node.consistentId();
    }

    /**
     * Returns collection of nodes (primary first) for specified partition.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param backups Number of backups.
     * @param neighborhoodCache Neighborhood.
     * @return Assignment.
     */
    public List<ClusterNode> assignPartition(int part,
                                             List<ClusterNode> nodes,
                                             int backups,
                                             @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
        if (nodes.size() == 1)
            return nodes;

        List<ClusterNode> lst = new ArrayList<>();
        int partitionsPerNode = 0;
        if(part < 1024){
         /*   Map<String, ClusterNode> masterNodes = new TreeMap<>();
            for(ClusterNode node: nodes){
                if(node.attribute("role").toString().equalsIgnoreCase("master")){
                    masterNodes.put(node.attribute("group").toString(), node);
                    *//*if(masterNodes.size() == 3){
                        break;
                    }*//*
                }
            }
            List<ClusterNode> masterList = new ArrayList<>(0);
            for(Map.Entry<String, ClusterNode> entry: masterNodes.entrySet()){
                System.out.println(entry.getValue());
                masterList.add(entry.getValue());
            }

            partitionsPerNode = (int) Math.ceil(1024 / (double) masterNodes.size());
            System.out.println(partitionsPerNode);
            nodes = masterList;*/
            partitionsPerNode = (int) Math.ceil(1024 / (double) nodes.size());
        }
        else {
            partitionsPerNode = parts / nodes.size();
        }

        //System.out.println("The size of nodes: "+nodes.size()+" and partitionsPerNode: "+partitionsPerNode);
        int index = 0;
        while (index < nodes.size()) {

            //System.out.println("The index size is: "+index + "and the parts is: "+part);
            if (part >= index * partitionsPerNode && part < (index + 1) * partitionsPerNode) {

                //System.out.println("Entered!");
                    lst.add(nodes.get(index));
                    break;
                }
            else{
                index++;
                //System.out.println("Increasing index size: "+index);
            }
        }
        return lst;
    }

    /**
     * Creates assignment for REPLICATED cache
     *
     * @param nodes Topology.
     * @param sortedNodes Sorted for specified partitions nodes.
     * @return Assignment.
     */
    private List<ClusterNode> replicatedAssign(List<ClusterNode> nodes, Iterable<ClusterNode> sortedNodes) {
        ClusterNode primary = sortedNodes.iterator().next();

        List<ClusterNode> res = new ArrayList<>(nodes.size());

        res.add(primary);

        for (ClusterNode n : nodes)
            if (!n.equals(primary))
                res.add(n);

        assert res.size() == nodes.size() : "Not enough backups: " + res.size();

        return res;
    }

    /**
     * The pack partition number and nodeHash.hashCode to long and mix it by hash function based on the Wang/Jenkins
     * hash.
     *
     * @param key0 Hash key.
     * @param key1 Hash key.
     * @see <a href="https://gist.github.com/badboy/6267743#64-bit-mix-functions">64 bit mix functions</a>
     * @return Long hash key.
     */
    private static long hash(int key0, int key1) {
        long key = (key0 & 0xFFFFFFFFL)
                | ((key1 & 0xFFFFFFFFL) << 32);

        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key ^= (key >>> 24);
        key += (key << 3) + (key << 8); // key * 265
        key ^= (key >>> 14);
        key += (key << 2) + (key << 4); // key * 21
        key ^= (key >>> 28);
        key += (key << 31);

        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("Null key is passed for a partition calculation. " +
                    "Make sure that an affinity key that is used is initialized properly.");


        //System.out.println("My partitions: "+ignite.affinity("STRETCH-CACHE").allPartitions(ignite.cluster().localNode()).length);

        //if(ignite.affinity("STRETCH-CACHE").mapPartitionToPrimaryAndBackups(keyToPartitionMap.get(p)).isEmpty()){}


        //Map<String, Integer> keyToPartitionMap = getUpdatedkeyToPartitionMap();



        //System.out.println("Key is "+key);
        String p = key.toString().substring(precision, precision + 2);
        //System.out.println("Portion of key is: "+p);
        if (keyToPartitionMap.containsKey(p)) {
            //System.out.println("The key is: "+p+" and partition is: "+keyToPartitionMap.get(p));


            /*if(keyToPartitionMap.get(p)==330){

                System.out.println("Inside foudnd 330 even after removable.");
            }*/

            return keyToPartitionMap.get(p);

        } else {
            for (int k = precision + 2; k < key.toString().length(); k++) {

                p += Character.toString(key.toString().charAt(k));
                if (keyToPartitionMap.containsKey(p)) {
                   //System.out.println("!!The key is: "+p+" and partition is: "+keyToPartitionMap.get(p));
                    return keyToPartitionMap.get(p);
                }
            }
        }
       // System.out.println("The key is: "+p+" and partition is: -1");
        return -1;
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {


        String event = affCtx.discoveryEvent().shortDisplay().split(":")[0];
        System.out.println(event);



        System.out.println("Hello");

        List<List<ClusterNode>> assignments = new ArrayList<>(parts);



        //String event = affCtx.discoveryEvent().shortDisplay().split(":")[0];

        //System.out.println("The event is: "+affCtx.discoveryEvent().shortDisplay().split(":")[0]);

        /*if(affCtx.discoveryEvent().shortDisplay().split(":")[0].equals("NODE_JOINED")) {

            System.out.println("The previous Assignment is: " + affCtx.previousAssignment(10));
            ClusterNode newlyJoinedNode = affCtx.discoveryEvent().eventNode();
            String donated = newlyJoinedNode.attribute("donated");
            List<ClusterNode> newList = new ArrayList<>();
            newList.add(newlyJoinedNode);
            System.out.println("Is the node donated: " + donated);
            if (donated.equals("yes")) {
                System.out.println("ENtering partiton movement!");
                //Hotspot info coming via config xml
                String hotspot_partitions = newlyJoinedNode.attribute("hotspot_partitions"); //separated by commas
                String[] partitionsToMove = hotspot_partitions.split(",");
                for (int k = 0; k < partitionsToMove.length; k++) {
                   System.out.println("The donation made: " + partitionsToMove[k]);
                }
                int j = 0;
                for (int i = 0; i < parts; i++) {
                    if (j < partitionsToMove.length) {
                        if (i == Integer.parseInt(partitionsToMove[j])) {
                            assignments.add(newList);
                            j++;
                            System.out.println("The node for moved partition id: " + i + " is: " + newlyJoinedNode);
                        } else {
                            System.out.println("The node for moved partition id: " + i + " is: " + affCtx.previousAssignment(i));
                            assignments.add(affCtx.previousAssignment(i));
                        }
                    } else {
                        System.out.println("The node for moved partition id: " + i + " is: " + affCtx.previousAssignment(i));
                        assignments.add(affCtx.previousAssignment(i));
                    }
                }
                return assignments;
            }
        }*/



        Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
                GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;

        List<ClusterNode> nodes = affCtx.currentTopologySnapshot();


        //System.out.println("#nodes"+nodes.size());
        ClusterNode newlyJoinedNode = affCtx.discoveryEvent().eventNode();
        int index=0;

        for(int i=0; i<nodes.size(); i++){

            if(nodes.get(i).equals(newlyJoinedNode)){
                index = i;
            }
        }




        String donated = newlyJoinedNode.attribute("donated");
        String splitCall = newlyJoinedNode.attribute("split");
        String newNodeID = newlyJoinedNode.id().toString();
        System.out.println("New node id: "+newNodeID);
        List<ClusterNode> splitNodeList = new ArrayList<>();

        //System.out.println("The partID for hotkey bbks: "+keyToPartitionMap.get("bbks"));

        boolean splitFlag = splitCall.equalsIgnoreCase("yes");
        int startSplit = -1;
        int endSplit = -1;
        int splitPartition = -1;

        if(splitFlag){
            System.out.println("Size of nodes:"+nodes.size());
            for(int i=0; i<nodes.size(); i++){
                System.out.println("The nodes are: "+nodes.get(i));
            }
            //nodes.remove(newlyJoinedNode);




            startSplit = Integer.parseInt(newlyJoinedNode.attribute("startSplit"));
            endSplit = Integer.parseInt(newlyJoinedNode.attribute("endSplit"));
            String splitNodeID = newlyJoinedNode.attribute("node");

            System.out.println("StartSplit: "+startSplit);
            System.out.println("EndSplit: "+endSplit);

            String path = newlyJoinedNode.attribute("map");
            int previousSize = keyToPartitionMap.size();
            Map<String, Integer> map = new HashMap<>();

            try {
                FileChannel channel1 = new RandomAccessFile(path, "rw").getChannel();
                FileLock lock = channel1.lock(); //Lock the file. Block until release the lock
                System.out.println("LOCKED IN AF.");
                ObjectInputStream ois = new ObjectInputStream(Channels.newInputStream(channel1));
                map = (HashMap<String, Integer>) ois.readObject();
                System.out.println("UNLOCKED IN AF.");
                keyToPartitionMap = map;
                lock.release();
                ois.close();
                channel1.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


            /*Set set = keyToPartitionMap.entrySet();
            Iterator iterator = set.iterator();
            while(iterator.hasNext()) {
                Map.Entry mentry = (Map.Entry)iterator.next();
                System.out.print("key: "+ mentry.getKey() + " & Value: ");
                System.out.println(mentry.getValue());
            }*/

            //System.out.println("Size of map is: "+keyToPartitionMap.size());
            //System.out.println("Hot key is: "+hotKey);
            //System.out.println("Hot key removed with value: "+keyToPartitionMap.remove(hotKey));
            System.out.println("Size of map is: "+keyToPartitionMap.size());

            for(int k=0; k< nodes.size(); k++){
                if(nodes.get(k).id().toString().equalsIgnoreCase(splitNodeID)){

                    System.out.println("Added to splitList: "+nodes.get(k).id());
                    splitNodeList.add(nodes.get(k));
                    break;
                }
            }

            //System.out.println("The partID for hotkey bbks: "+keyToPartitionMap.get("bbks"));
        }




        //parts = keyToPartitionMap.size();
        //System.out.println("The size of parts is: "+parts);
        List<ClusterNode> newList = new ArrayList<>();
        //newList.add(newlyJoinedNode);
        int[] partitionsToMoveAscending = null;
        String causeOfHotspot = "M";

        //System.out.println("Is the node donated: " + donated);


        if (donated.equals("yes")) {

            //System.out.println("The size of nodes is: "+nodes.size());
            nodes.remove(newlyJoinedNode);
            //System.out.println("The size of nodes is: "+nodes.size());


            //System.out.println( "ENtering partiton movement!");
            //Hotspot info coming via config xml
            String hotspot_partitions = newlyJoinedNode.attribute("hotspot-partitions"); //separated by commas
            String[] partitionsToMove = hotspot_partitions.split(",");

            System.out.println("Partitions to move: "+hotspot_partitions);
            System.out.println("Partitons to move: count "+partitionsToMove.length);

           //System.out.println("LEN: "+partitionsToMove.length);


            String idleNodeID = newlyJoinedNode.attribute("idle");
            causeOfHotspot = newlyJoinedNode.attribute("cause");
            //System.out.println("The idle node to use: "+idleNodeID);

            for(int k=0; k< nodes.size(); k++){
                if(nodes.get(k).id().toString().equals(idleNodeID)){

                    //System.out.println("Added to newList: "+nodes.get(k).id());
                    newList.add(nodes.get(k));
                    break;
                }
            }

            int[] partitionsToMoveAscending1 = new int[partitionsToMove.length];

            for (int k = 0; k < partitionsToMove.length; k++) {
                if(!partitionsToMove[k].isEmpty()){
                    //System.out.println(""+k+". "+partitionsToMove[k].trim());
                    partitionsToMoveAscending1[k] = Integer.parseInt(partitionsToMove[k].trim());
                }
            }

            Arrays.sort(partitionsToMoveAscending1);

            partitionsToMoveAscending = partitionsToMoveAscending1;

        }

        System.out.println(partitionsToMoveAscending);



        boolean flag = donated.equals("yes");

        /*if(!event.equals("NODE_LEFT")){

            if(!(flag || splitFlag)){
                initializeKeyToPartitionMap();
            }

        }*/


        //System.out.println("flag: "+flag);
        int j = 0;
        System.out.println(parts);
        for (int i = 0; i < parts; i++) {

            if (flag && j < partitionsToMoveAscending.length) {
                    if (i == partitionsToMoveAscending[j]) {
                        j++;
                        if(causeOfHotspot.equalsIgnoreCase("CM")){
                            /*List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache);
                            partAssignment.add(newList.get(0));*/
                            assignments.add(newList);
                        }else if(causeOfHotspot.equalsIgnoreCase("C")){
                            assignments.add(newList);
 /*                           List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache);
                            partAssignment.add(newList.get(0));
*/
                        }else if(causeOfHotspot.equalsIgnoreCase("M")){
                            //System.out.println(""+i+"Movement");
                            //System.out.println(""+j);
                            assignments.add(newList);
                        }
                    } else {


                        if(event.equals("NODE_LEFT")){

                            assignments.add(affCtx.previousAssignment(i));
                        }
                        else{
                            List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache);
                            assignments.add(partAssignment);
                        }
                        //System.out.println("The node for moved partition id: " + i + " is: " + affCtx.previousAssignment(i));
                        //assignments.add(affCtx.previousAssignment(i));
                        //System.out.println(""+i+"Entering partiton assignment after mismatch.");
                        //List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache);

                    }
            }
            else if(splitFlag && (i>=startSplit && i<=endSplit)){

                assignments.add(splitNodeList);
                //System.out.println("The partitions "+i+" is set to: "+splitNodeList.get(0).id()+" from previous assignment: "+affCtx.previousAssignment(i).get(0).id());
            }else{
                if(event.equals("NODE_LEFT")){

                    assignments.add(affCtx.previousAssignment(i));
                }
                else{
                    List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups(), neighborhoodCache);
                    assignments.add(partAssignment);
                }
            }
        }


        System.out.println(assignments.size());

        System.out.println("Return assignments");


        //getUpdatedkeyToPartitionMap


        return assignments;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /**
     *
     */
    private static class HashComparator implements Comparator<IgniteBiTuple<Long, ClusterNode>>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public int compare(IgniteBiTuple<Long, ClusterNode> o1, IgniteBiTuple<Long, ClusterNode> o2) {
            return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                    o1.get2().id().compareTo(o2.get2().id());
        }
    }

    /**
     * Sorts the initial array with linear sort algorithm array
     */
    private static class LazyLinearSortedContainer implements Iterable<ClusterNode> {
        /** Initial node-hash array. */
        private final IgniteBiTuple<Long, ClusterNode>[] arr;

        /** Count of the sorted elements */
        private int sorted;

        /**
         * @param arr Node / partition hash list.
         * @param needFirstSortedCnt Estimate count of elements to return by iterator.
         */
        LazyLinearSortedContainer(IgniteBiTuple<Long, ClusterNode>[] arr, int needFirstSortedCnt) {
            this.arr = arr;

            if (needFirstSortedCnt > (int)Math.log(arr.length)) {
                Arrays.sort(arr, COMPARATOR);

                sorted = arr.length;
            }
        }

        /** {@inheritDoc} */
        @Override public Iterator<ClusterNode> iterator() {
            return new SortIterator();
        }

        /**
         *
         */
        private class SortIterator implements Iterator<ClusterNode> {
            /** Index of the first unsorted element. */
            private int cur;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return cur < arr.length;
            }

            /** {@inheritDoc} */
            @Override public ClusterNode next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (cur < sorted)
                    return arr[cur++].get2();

                IgniteBiTuple<Long, ClusterNode> min = arr[cur];

                int minIdx = cur;

                for (int i = cur + 1; i < arr.length; i++) {
                    if (COMPARATOR.compare(arr[i], min) < 0) {
                        minIdx = i;

                        min = arr[i];
                    }
                }

                if (minIdx != cur) {
                    arr[minIdx] = arr[cur];

                    arr[cur] = min;
                }

                sorted = cur++;

                return min.get2();
            }

            /** {@inheritDoc} */
            @Override public void remove() {
                throw new UnsupportedOperationException("Remove doesn't supported");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StretchAffinityFunctionXX.class, this);
    }
}
