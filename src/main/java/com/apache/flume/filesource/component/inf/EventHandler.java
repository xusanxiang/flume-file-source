/******************************************************************************
 * Copyright (C) 2014 ShenZhen Oneplus Technology Co.,Ltd
 * All Rights Reserved.
 * 本软件为深圳万普拉斯科技有限公司开发研制。未经本公司正式书面同意，其他任何个人、团体不得使用、复制、修改或发布本软件.
 *****************************************************************************/
package com.apache.flume.filesource.component.inf;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.flume.Event;

/**
 * @author XuYexin
 * @Description :事件处理器
 * @date 2016/8/18
 */
public interface EventHandler {
    /**
     * 创建Event
     * @return
     */
    boolean createEvent();

    /**
     * 发送event到channel
     * @return
     */
    boolean sendEvent();

    /**
     * 清空当前处理器缓存
     */
    void clear();

    void setBuffer(ByteBuffer buffer);

    List<Event> getEventList();

    int addReadBytes(int bytes);

    int getReadBytes();

    public void setReadBytes(int readBytes);
}
