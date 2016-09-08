/******************************************************************************
 * Copyright (C) 2014 ShenZhen Oneplus Technology Co.,Ltd
 * All Rights Reserved.
 * 本软件为深圳万普拉斯科技有限公司开发研制。未经本公司正式书面同意，其他任何个人、团体不得使用、复制、修改或发布本软件.
 *****************************************************************************/
package com.apache.flume.filesource.component;

import com.apache.flume.filesource.common.Constants;
import com.apache.flume.filesource.util.TimeUtil;
import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author FenZhiHao
 * @Description :多行事件处理器
 * @date 2016/8/18
 */
public class MultiLineEventHandler extends DefaultEventHandler {

    private static final Logger log = LoggerFactory.getLogger(MultiLineEventHandler.class);

    private String lineStartRegex;

    private Pattern pattern;

    private final List<String> bufferedLines = Lists.newArrayList();

    /**
     * @param channelProcessor
     * @param filePath
     */
    public MultiLineEventHandler(ChannelProcessor channelProcessor, String filePath, String lineStartRegex) {
        super(channelProcessor, filePath);
        this.lineStartRegex = lineStartRegex;
        this.pattern = Pattern.compile(lineStartRegex);
    }

    @Override
    public boolean createEvent() {
        int bufferGetCount = 0;
        int lineEndPosition = -1;
        byte[] bufferArray = new byte[BUFFER_SIZE];
        ByteBuffer bf = getBuffer();
        bf.flip();
        String currentLine;
        Matcher m;
        while (bf.hasRemaining()) {
            byte tempByte = bf.get();
            bufferGetCount++;
            bufferArray[bufferGetCount - 1] = tempByte;
            if (tempByte == (byte) '\n') {
                // read one line
                byte[] lineArray = new byte[bufferGetCount - 1 - lineEndPosition];
                System.arraycopy(bufferArray, lineEndPosition + 1, lineArray, 0, bufferGetCount - 1 - lineEndPosition);
                lineEndPosition = bufferGetCount - 1;
                currentLine = new String(lineArray);

                m = pattern.matcher(currentLine);
                if (m.find()) {
                    // if current line can regex with the rule we configed,then
                    // combined all lines buffer as a event body
                    if (bufferedLines.size() != 0) {
                        // write to body
                        String total = "";
                        for (int i = 0; i < bufferedLines.size(); ++i) {
                            total += bufferedLines.get(i);
                        }
                        getEventHeader().put(Constants.HEADER_TIME, TimeUtil.getTime(m.group()));
                        Event event = EventBuilder.withBody(total.getBytes(), getEventHeader());
                        getEventList().add(event);
                        bufferedLines.clear();
                    }
                }
                bufferedLines.add(currentLine);
            }
        }

        // 没有生成event输出错误的buffer内容
        if (0 == this.getEventList().size()) {
            log.error("No Line In buffer !!! File:" + getFilePath() + ",This Buffer size:" + bufferGetCount);
            return false;
        }
        return true;
    }

    public String getLineStartRegex() {
        return lineStartRegex;
    }

    public void setLineStartRegex(String lineStartRegex) {
        this.lineStartRegex = lineStartRegex;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public void setPattern(Pattern pattern) {
        this.pattern = pattern;
    }

    public static void main(String[] args) {
        String currentContent = "2016-08-22 17:52:41.121";
        String lineStartRegex = "^\\s*\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d.\\d\\d\\d";
        Pattern pattern = Pattern.compile(lineStartRegex);
        System.out.println(pattern.matcher(currentContent).find());
    }
}
