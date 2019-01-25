package edu.colostate.cs.fa2017.stretch.util;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class Cluster2 {
    public static void main (String[] args){
        Ignite ignite = Ignition.start("./config/util/Cluster2.xml");

    }

}
