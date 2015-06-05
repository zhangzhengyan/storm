package	cn.polly.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LogCounter extends BaseBasicBolt {
	private HashMap<String, Integer> counters;
	BufferedWriter output;
	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) 
	{
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String ip = input.getString(0);

		/**
		 * If the word dosn't exist in the map we will create
		 * this, if not We will add 1 
		 */
		Integer count;
		
		if(!this.counters.containsKey(ip)){
			this.counters.put(ip, 1);
		}else{
			count = this.counters.get(ip) + 1;
			this.counters.put(ip, count);
		}	
		
		Iterator<String> iterator = this.counters.keySet().iterator();
		try {
			output = new BufferedWriter(new FileWriter("./result.txt", false));
		}
		catch(IOException e) {
			e.printStackTrace();
			
			try {
				output.close();
			}
			catch(IOException e1) {
				e1.printStackTrace();
			}
		}
		
		while(iterator.hasNext())
		{
			String next = iterator.next();
			
			try {
				output.write(next+":"+this.counters.get(next));
				output.newLine();
				output.flush();
			}
			catch(IOException e) {
				e.printStackTrace();
				
				try {
					output.close();
				}
				catch(IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	
	public void cleanup() {
		System.out.println("--- FINAL COUNTS ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counters.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			System.out.println(key + " : " + this.counters.get(key));
		}
		System.out.println("--------------");
	}
	
}
