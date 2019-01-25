package examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterStartNodeResult;

import java.io.File;
import java.util.Collection;

public class StartUsingFile {

    public static void main(String[] args){

        Ignite ignite = Ignition.start();
        File file = new File("./config/INI/start.ini");


        Collection<ClusterStartNodeResult> result = ignite.cluster().startNodes(file, false, 1000, 5);
        for (ClusterStartNodeResult res : result) {
            if (!res.isSuccess()) {
                System.out.println("Failed to start.");
            } else {
                System.out.println("Ignite server start successfully triggered on machine " + res.getHostName());
            }
        }


    }
}
