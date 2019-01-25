package examples;

import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterMetrics;

public class ResourceRequest extends Thread {

    private static final String REQUEST_TOPIC = "Resource_Requested";
    private IgniteMessaging igniteMessaging;
    private ClusterMetrics clusterMetrics;

    public ResourceRequest(IgniteMessaging igniteMessaging, ClusterMetrics clusterMetrics){

        this.igniteMessaging = igniteMessaging;
        this.clusterMetrics = clusterMetrics;

    }


    @Override
    public void run() {

        while (true) {

            if (clusterMetrics.getCurrentActiveJobs() < 100) {

                String myStatus = "Heap memory in my cluster is getting low.";
                igniteMessaging.send(REQUEST_TOPIC, myStatus);
                try {
                    sleep(15000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
