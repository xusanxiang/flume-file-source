package com.apache.flume.filesource.component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apache.flume.filesource.util.IpUtil;
import com.apache.flume.filesource.component.inf.EventHandler;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apache.flume.filesource.common.Constants;

/**
 * @author XuYexin
 * @Description :单行事件处理器
 * @date 2016/8/18
 */
public class DefaultEventHandler implements EventHandler {
    private List<Event> eventList;
    private ByteBuffer buffer;
    private ChannelProcessor channelProcessor;
    public static int BUFFER_SIZE = 1 << 20;
    private Map<String, String> eventHeader;
    private int readBytes;
    private final String filePath;
    private static final Logger log = LoggerFactory.getLogger(DefaultEventHandler.class);


    public DefaultEventHandler(ChannelProcessor channelProcessor, String filePath) {
        this.readBytes = 0;
        this.eventList = new ArrayList<Event>();
        this.buffer = null;
        this.channelProcessor = channelProcessor;
        this.eventHeader = new HashMap<String, String>();
        this.filePath = filePath;
        initHeader();
    }

    @Override
    public void clear() {
        this.readBytes = 0;
        this.eventList = new ArrayList<Event>();
        this.buffer = null;
    }

    @Override
    public boolean createEvent() {
        int bufferGetCount = 0;
        int lineEndPosition = -1;
        byte[] bufferArray = new byte[BUFFER_SIZE];
        buffer.flip();
        while (buffer.hasRemaining()) {
            byte tempByte = buffer.get();
            bufferGetCount++;
            bufferArray[bufferGetCount - 1] = tempByte;
            if (tempByte == (byte) '\n') {
                byte[] lineArray = new byte[bufferGetCount - 1 - lineEndPosition];
                System.arraycopy(bufferArray, lineEndPosition + 1, lineArray, 0, bufferGetCount - 1 - lineEndPosition);
                lineEndPosition = bufferGetCount - 1;
                Event event = EventBuilder.withBody(lineArray, eventHeader);
                this.eventList.add(event);
            }
        }

        // 没有生成event输出错误的buffer内容
        if (0 == this.eventList.size()) {
            log.error("No Line In buffer !!! File:" + filePath + ",This Buffer size:" + bufferGetCount);
            return false;
        }
        return true;
    }


    @Override
    public boolean sendEvent() {
        log.debug("Event handler send event size :" + eventList.size());
        for (Event event : this.eventList) {
            try {
                channelProcessor.processEvent(event);
                readBytes += event.getBody().length;
            } catch (Exception e) {
                log.error("push event to channel error", e);
                //stop send event and now read bytes = event already send size
                return false;
            }
        }
        return true;
    }

    @Override
    public List<Event> getEventList() {
        return eventList;
    }

    @Override
    public int addReadBytes(int bytes) {
        this.readBytes += bytes;
        return this.readBytes;
    }

    @Override
    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private void initHeader() {
        eventHeader.put(Constants.HEADER_HOST, IpUtil.getLocalIp());
        eventHeader.put(Constants.HEADER_FILE, filePath);
    }

    public void setEventList(List<Event> eventList) {
        this.eventList = eventList;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public ChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    @Override
    public int getReadBytes() {
        return readBytes;
    }

    public void setReadBytes(int readBytes) {
        this.readBytes = readBytes;
    }

    public void setChannelProcessor(ChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
    }

    public Map<String, String> getEventHeader() {
        return eventHeader;
    }

    public void setEventHeader(Map<String, String> eventHeader) {
        this.eventHeader = eventHeader;
    }

    public String getFilePath() {
        return filePath;
    }

}
