package com.apache.flume.filesource.test;

import com.apache.flume.filesource.FileSource;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestFileSource {

	private FileSource source;
	private Context context;
	private Channel channel;
	private ChannelSelector rcs = new ReplicatingChannelSelector();
	
	@Before
	public void before() {
		this.context = new Context();
		this.context.put("file", TestConstants.FILE);
		this.context.put("positionDir", TestConstants.POSITION_DIR);
	
		source = new FileSource();
		channel = new MemoryChannel();
		rcs.setChannels(Lists.newArrayList(channel));
		source.setChannelProcessor(new ChannelProcessor(rcs));
	}
	
	@Test
	public void test() {
		source.configure(context);
		source.start();
	}
	
}


























