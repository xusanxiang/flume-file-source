package com.apache.flume.filesource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.apache.flume.filesource.component.DefaultEventHandler;
import com.apache.flume.filesource.component.PositionRecorder;
import com.apache.flume.filesource.exception.FileSourceException;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.apache.flume.filesource.common.Constants;
import com.apache.flume.filesource.component.inf.EventHandler;
import com.apache.flume.filesource.component.MultiLineEventHandler;

public class FileSource extends AbstractSource implements Configurable, EventDrivenSource {

	private static final Logger log = LoggerFactory.getLogger(FileSource.class);
	private ChannelProcessor channelProcessor;
	private RandomAccessFile monitorFile = null;
	private File coreFile = null;
	private FileChannel monitorFileChannel = null;
	private String monitorFilePath = null;
	private String positionFilePath = null;
	private long lastMod = 0L;
	private long lastFileSize = 0L;
	private long nowFileSize = 0L;
	private long positionValue = 0L;
	private long readCount = 0l;
	private long eventCount = 0l;
	public static int BUFFER_SIZE = 1 << 20;
	private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);// 1MB
	private ScheduledExecutorService executor;
	private FileMonitorThread runner;
	private final Object exeLock = new Object();
	private PositionRecorder positionRecorder = null;
	private SourceCounter sourceCounter;
    private EventHandler eventHandler;

    private String regex;

	@Override
	public synchronized void start() {
		channelProcessor = getChannelProcessor();
        if (regex == null) {
            eventHandler = new DefaultEventHandler(channelProcessor, monitorFilePath);
            log.info("Create DefaultEventHandler !");
        } else {
            eventHandler = new MultiLineEventHandler(channelProcessor, monitorFilePath, regex);
            log.info("Create MultiLineEventHandler !");
        }

		executor = Executors.newSingleThreadScheduledExecutor();
		runner = new FileMonitorThread();
		executor.scheduleWithFixedDelay(runner, 500, 1000, TimeUnit.MILLISECONDS);
		sourceCounter.start();
		super.start();
		log.info("File source {} start", this.coreFile.getName());
	}

	@Override
	public synchronized void stop() {
		positionRecorder.setPosition(positionValue);
		log.debug("Set the positionValue {} when stopped", positionValue);
		if (this.monitorFileChannel != null) {
			try {
				this.monitorFileChannel.close();
			} catch (IOException e) {
				log.error(this.monitorFilePath + " file channel close Exception", e);
			}
		}
		if (this.monitorFile != null) {
			try {
				this.monitorFile.close();
			} catch (IOException e) {
				log.error(this.monitorFilePath + " file close Exception", e);
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			log.info("Interrupted while awaiting termination", ex);
		}
		executor.shutdownNow();
		sourceCounter.stop();
		super.stop();
		log.info("File source {} stopped", this.coreFile.getName());
	}

    @Override
	public void configure(Context context) {
		this.monitorFilePath = context.getString(Constants.MONITOR_FILE);
		this.positionFilePath = context.getString(Constants.POSITION_DIR);
        this.regex = context.getString(Constants.LINE_REGEX, null);

		Preconditions.checkArgument(monitorFilePath != null, "The file can not be null !");
		Preconditions.checkArgument(positionFilePath != null, "the positionDir can not be null !");
		if (positionFilePath.endsWith(":")) {
			positionFilePath += File.separator;
		} else if (positionFilePath.endsWith("\\") || positionFilePath.endsWith("/")) {
			positionFilePath = positionFilePath.substring(0, positionFilePath.length() - 1);
		}
		// create properties file when start the source if the properties is not exists
		File file = new File(positionFilePath + File.separator + Constants.POSITION_FILE_NAME);
		if (!file.exists()) {
			try {
				file.createNewFile();
				log.debug("Create the {} file:{}", Constants.POSITION_FILE_NAME, file.getPath());
			} catch (IOException e) {
				String errMsg = "Create the position.properties error";
				log.error(errMsg, e);
				throw new FileSourceException(errMsg);
			}
		}
		try {
			coreFile = new File(monitorFilePath);
			lastMod = coreFile.lastModified();
		} catch (Exception e) {
			String errMsg = "Initialize the File/FileChannel Error";
			log.error(errMsg, e);
			throw new FileSourceException(errMsg);
		}

		positionRecorder = new PositionRecorder(positionFilePath);
		try {
			positionValue = positionRecorder.initPosition();
		} catch (Exception e) {
			String errMsg = "Initialize the positionValue in File positionLog";
			log.error(errMsg, e);
			throw new FileSourceException(errMsg);
		}
		lastFileSize = positionValue;
		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
	}

	class FileMonitorThread implements Runnable {

		/**
		 * a thread to check whether the file is modified
		 */
        @Override
		public void run() {
			synchronized (exeLock) {
				String fileName = coreFile.getName();
				long nowModified = coreFile.lastModified();
				// the file has been changed
				if (lastMod != nowModified) {
					log.debug("File {} has been modified, start to read", fileName);
					lastMod = nowModified;
					nowFileSize = coreFile.length();
					int readDataBytesLen = 0;
					try {
						log.debug("File " + fileName + " last size {}, now size {}", lastFileSize, nowFileSize);
						// it indicated the file is rolled by log4j
						if (nowFileSize <= lastFileSize) {
							log.info("File {} size has been rolled, reset position to 0L", fileName);
							positionValue = 0L;
						}
						lastFileSize = nowFileSize;
						monitorFile = new RandomAccessFile(coreFile, "r");
						monitorFileChannel = monitorFile.getChannel();
						monitorFileChannel.position(positionValue);
						log.debug("File {} current read position:{}", fileName, positionValue);
						int bytesRead = monitorFileChannel.read(buffer);
						if (bytesRead != -1) {
							log.debug("File {} source read bytes count :{}", fileName, bytesRead);
							eventHandler.setBuffer(buffer);
							boolean createFlag = eventHandler.createEvent();
							if (createFlag) {
								eventHandler.sendEvent();
                                eventCount += eventHandler.getEventList().size();
							} else {
                                // 没能成功创建event跳过这个buffer
                                eventHandler.addReadBytes(bytesRead);
							}

							if (readCount % Constants.PRINT_COUNT == 0) {
								log.info("Source " + fileName + " => push count:{} ,event count :{}",
									readCount, eventCount);
								eventCount = 0L;
							}

                            log.debug("File {} source push event size :{}", fileName, eventHandler.getEventList().size());
                            log.debug("File {} source push event bytes :{}", fileName, eventHandler.getReadBytes());

                            if (eventHandler.getReadBytes() != 0) {
                                positionValue += eventHandler.getReadBytes();
                                readDataBytesLen += eventHandler.getReadBytes();
							}
							eventHandler.clear();
							sourceCounter.incrementEventReceivedCount();
							sourceCounter.addToEventAcceptedCount(1);
							long fillPositionRecorder = positionRecorder.setPosition(positionValue);
							log.debug("File {} change the read position ", fileName, positionValue);
							log.debug("File {} change the position in recorder file :{}", fileName,
								fillPositionRecorder);

							buffer.clear();
							readCount++;
						}
					} catch (Exception e) {
						log.error("{} Read data into Channel Error", fileName, e);
						log.debug("Save the last positionValue {} into Disk File", positionValue
							- readDataBytesLen);
						positionRecorder.setPosition(positionValue - readDataBytesLen);
					}finally {
                        try {
							monitorFile.close();
                            monitorFileChannel.close();
                        } catch (IOException e) {
                            log.error("Close {} file and channel error : {}", fileName, e);
                        }
                    }
                }
			}
		}

	}

}
