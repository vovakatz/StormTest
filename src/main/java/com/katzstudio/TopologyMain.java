package com.katzstudio;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new yfSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new yfBolt())
            .shuffleGrouping("Yahoo-Finance-Spout");
        builder.setBolt("Yahoo-Finance-Bolt2", new yfBolt2())
                .shuffleGrouping("Yahoo-Finance-Bolt");


        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Stock-Tracker-Topology", conf, topology);
        } finally {
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
