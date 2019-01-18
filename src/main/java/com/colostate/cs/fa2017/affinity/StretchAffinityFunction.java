package com.colostate.cs.fa2017.affinity;

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Logger;

public class StretchAffinityFunction implements AffinityFunction, Serializable {

    //public AffinityFunctionContext affinityFunctionContext;

    private final static Logger LOGGER = Logger.getLogger(StretchAffinityFunction.class.getName());


    private static final char[] base32 = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f',
            'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

    private Map<String, Collection<ClusterNode>> subClusterInfo;

    /** Number of partitions. */
    private int parts = 10;

    /** Mask to use in calculation when partitions count is power of 2. */
    private int mask = -1;

    private static final Comparator<IgniteBiTuple<Long, ClusterNode>> COMPARATOR = new HashComparator();


//    public StretchAffinityFunction() {
//
//    }

    public StretchAffinityFunction(int parts) {


        //LOGGER.warning("STRETCH_AFFINITY_FUNCTION!!!!!!!!!!!");

        A.ensure(parts > 0, "parts > 0");
        setPartitions(parts);
        //this.subClusterInfo = subClusterInfo;
    }


    public StretchAffinityFunction setPartitions(int parts) {
        A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT,
                "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);
        A.ensure(parts > 0, "parts > 0");

        this.parts = parts;

        mask = (parts & (parts - 1)) == 0 ? parts - 1 : -1;

        return this;
    }





    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key == null)
            throw new IllegalArgumentException("Null key is passed for a partition calculation. " +
                    "Make sure that an affinity key that is used is initialized properly.");

        /*if (mask >= 0) {
            int h;

            return ((h = key.hashCode()) ^ (h >>> 16)) & mask;
        }

        return U.safeAbs(key.hashCode() % parts);*/
//        char k = key.toString().charAt(0);
//        int part = 0;
//
//        for (int i=0; i< base32.length; i++) {
//            if (k == base32[i]) {
//                part = i;
//                break;
//            }
//        }
        //LOGGER.warning("STRETCH_AFFINITY_FUNCTION!!!!!!!!!!!");
        //LOGGER.warning("STRETCH_AFFINITY_FUNCTION!!!!!!!!!!!"+part);

        int part = Integer.parseInt(key.toString()) % parts;

        System.out.println("The key to partition map: key| "+key+" ==> partition| "+part);


        return part;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    //public AffinityFunctionContext getAffinityFunctionContext() {
//        return affinityFunctionContext;
//    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {


       // affinityFunctionContext = affCtx;

        //affCtx.previousAssignment()
        //affCtx.previousAssignment()
        List<List<ClusterNode>> assignments = new ArrayList<>(parts);

        /*Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
                GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;*/


        List<ClusterNode> nodes = affCtx.currentTopologySnapshot();


        System.out.println("The topology version is: "+affCtx.currentTopologyVersion());

//

       System.out.println("The discovery event short display is: "+affCtx.discoveryEvent().shortDisplay());
       System.out.println("The event is: "+affCtx.discoveryEvent().shortDisplay().split(":")[0]);
        System.out.println("Is the event: "+affCtx.discoveryEvent().shortDisplay().split(":")[0].equals("NODE_JOINED"));




        if(!affCtx.discoveryEvent().shortDisplay().split(":")[0].equals("NODE_JOINED")){
            for(int j=0; j<parts; j++){
                List<ClusterNode> previousPartAssignment = affCtx.previousAssignment(j);
                assignments.add(previousPartAssignment);
            }

            System.out.println("Except Node join case.");
            return assignments;
        }

        ClusterNode newlyJoined = affCtx.discoveryEvent().eventNode();

        String newHost = newlyJoined.hostNames().iterator().next();



        Iterator<ClusterNode> it = affCtx.currentTopologySnapshot().iterator();
        while(it.hasNext()){
            ClusterNode tmp = it.next();
            String tmpHost = tmp.hostNames().iterator().next();

            if (tmpHost.equals(newHost) && !tmp.id().equals(newlyJoined.id())){


                //backup the older data

            }


        }






        //LOGGER.warning("Logging an INFO-level message");
        //LOGGER.warning("The discovery event short display is: "+affCtx.discoveryEvent().shortDisplay());


        //nodes.get(0);

//        Collection<ClusterNode> workerA = new ArrayList<ClusterNode>() {{
//                    add(nodes.get(0));
//                   // add(nodes.get(1));
//                }};

//        Collection<ClusterNode> workerB = new ArrayList<ClusterNode>() {{
//            add(nodes.get(1));
//            //add(nodes.get(3));
//        }};

       // subClusterInfo.put("A", workerA);
       // subClusterInfo.put("B", workerB);






        for (int i = 0; i < parts; i++) {
            List<ClusterNode> partAssignment = assignPartition(i, nodes, affCtx.backups());

            assignments.add(partAssignment);
        }

        return assignments;
    }




    /**
     * Returns collection of nodes (primary first) for specified partition.
     *
     * @param part Partition.
     * @param nodes Nodes.
     * @param backups Number of backups.
     * @return Assignment.
     */
    public List<ClusterNode> assignPartition(int part,
                                             List<ClusterNode> nodes,
                                             int backups) {
        if (nodes.size() < 1)
            return nodes;

        final int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());
        List<ClusterNode> res = new ArrayList<>(primaryAndBackups);

        int partsPerNode = parts/nodes.size();

        char[] groupName = {'A', 'B', 'C', 'D'};

//        for(int i = 0; i< subClusterInfo.size(); i++){
//
//            Collection<ClusterNode> nodesPerGroup = subClusterInfo.get(groupName[i]);
//            Iterator<ClusterNode> it = nodesPerGroup.iterator();
//            int partsPerGroup = partsPerNode * nodesPerGroup.size();
//            for(int j=0; j< nodesPerGroup.size(); j++){
//
//                int min = j * partsPerNode;
//                int max = (j+1) * partsPerNode;
//                if(part>=min && part < max){
//                    while(it.hasNext()){
//
//                        res.add(it.next());
//                        break;
//                    }
//
//                }
//            }
//        }


        int size = nodes.size();

        int index =  part % size;
        System.out.println("The partition tp node map: part= "+part+" goes to node= "+nodes.get(index));

        res.add(nodes.get(index));
        return res;






/*        IgniteBiTuple<Long, ClusterNode>[] hashArr =
                (IgniteBiTuple<Long, ClusterNode> [])new IgniteBiTuple[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode node = nodes.get(i);

            Object nodeHash = resolveNodeHash(node);

            long hash = hash(nodeHash.hashCode(), part);

            hashArr[i] = F.t(hash, node);
        }



        Iterable<ClusterNode> sortedNodes = new LazyLinearSortedContainer(hashArr, primaryAndBackups);

        // REPLICATED cache case
        if (backups == Integer.MAX_VALUE)
            return replicatedAssign(nodes, sortedNodes);

        Iterator<ClusterNode> it = sortedNodes.iterator();


        Collection<ClusterNode> allNeighbors = new HashSet<>();

        ClusterNode primary = it.next();

        res.add(primary);

        if (exclNeighbors)
            allNeighbors.addAll(neighborhoodCache.get(primary.id()));

        // Select backups.
        if (backups > 0) {
            while (it.hasNext() && res.size() < primaryAndBackups) {
                ClusterNode node = it.next();

                if (exclNeighbors) {
                    if (!allNeighbors.contains(node)) {
                        res.add(node);

                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                    }
                }
                else if ((backupFilter != null && backupFilter.apply(primary, node))
                        || (affinityBackupFilter != null && affinityBackupFilter.apply(node, res))
                        || (affinityBackupFilter == null && backupFilter == null) ) {
                    res.add(node);

                    if (exclNeighbors)
                        allNeighbors.addAll(neighborhoodCache.get(node.id()));
                }
            }
        }

        if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
            // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
            it = sortedNodes.iterator();

            it.next();

            while (it.hasNext() && res.size() < primaryAndBackups) {
                ClusterNode node = it.next();

                if (!res.contains(node))
                    res.add(node);
            }

            if (!exclNeighborsWarn) {
                LT.warn(log, "Affinity function excludeNeighbors property is ignored " +
                                "because topology has no enough nodes to assign backups.",
                        "Affinity function excludeNeighbors property is ignored " +
                                "because topology has no enough nodes to assign backups.");

                exclNeighborsWarn = true;
            }
        }

        assert res.size() <= primaryAndBackups;

        return res;*/
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
            return new LazyLinearSortedContainer.SortIterator();
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




}
