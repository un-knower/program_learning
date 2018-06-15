package demo.storm.wordcount2;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReaderSpout extends BaseRichSpout {
	private String inpath;
	private SpoutOutputCollector collector;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		inpath = (String) conf.get("INPUT_PATH");
	}

	@Override
	public void nextTuple() {
		//列出目录inpath下，除了.bak文件结尾的文件，不递归遍历目录
		Collection<File> files = FileUtils.listFiles(new File(inpath), FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")), null);
		for(File file : files) {
			try {
				List<String> lines = FileUtils.readLines(file, "UTF-8");
				for (String line : lines) {
					this.collector.emit(new Values(line));
				}
				//将读取过的文件改名为"file时间戳.bak"
				FileUtils.moveFile(file, new File(file.getPath()+System.currentTimeMillis()+".bak"));
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
