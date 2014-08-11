package br.com.quickmobile.helloworldstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import java.util.Map;

public class PythonTopology 
{
    public static class ExclamBolt extends ShellBolt implements IRichBolt 
    {
        public ExclamBolt() 
        {
          super("python", "exclamBolt.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) 
        {
          declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() 
        {
          return null;
        }
  }
    
    public static class MySpout extends ShellSpout implements IRichSpout{

        public MySpout() {
              super("python", "mySpout.py");
        }

        
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            ofd.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
        
    }

    public static void main(String[] args) throws Exception 
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new MySpout(), 1);
        builder.setBolt("exclaim1", new PythonTopology.ExclamBolt(), 1).shuffleGrouping("word");


        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
          conf.setNumWorkers(1);
          StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
        else {
          conf.setMaxTaskParallelism(3);

          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("word-count", conf, builder.createTopology());

          Thread.sleep(10000);

          cluster.shutdown();
        }
      }
}