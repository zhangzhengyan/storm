package	cn.polly.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;

public class TopologyMain {
	public static void main(String[] args) throws Exception {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("log-reader", new LogReader());
		builder.setBolt("log-normalizer", new LogNormalizer())
			.shuffleGrouping("log-reader");
		builder.setBolt("log-counter", new LogCounter())
			//.shuffleGrouping("log-normalizer");
			.fieldsGrouping("log-normalizer", new Fields("ipstr"));
		
        //Configuration
		Config conf = new Config();
		conf.put("logFile", args[0]);
		conf.setDebug(false);
        //Topology run
		if(args != null && args.length > 1)
		{
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(args[1], conf, builder.createTopology());
		}
		else
		{
			conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
			Thread.sleep(5000);
			cluster.shutdown();
		}
	}
}
