package org.momacmo.aws.s3;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.event.SyncProgressListener;

public class S3ProgressListener extends SyncProgressListener {

	long length;
	long bytes;
	float pct;
	long time;
	float pctinc;
	long timeinc;
	float lastPct;
	long lastTime;
	Logger logger;
	Level level;
	String prefix;
	
	public S3ProgressListener( long transferLength, float pctIncrement, float timeIncrementSec, Logger progressLogger, String msgPrefix ) {
		length = transferLength;
		pctinc = pctIncrement;
		timeinc = (long) (1000*timeIncrementSec);
		lastPct = 0;
		lastTime = System.currentTimeMillis();
		logger = progressLogger;
		if (logger == null) logger = Logger.getGlobal();
		level = logger.getLevel();
		if (level == null) level = Level.INFO;
		prefix = msgPrefix;
		reset(1);
	}
	
	public S3ProgressListener( long transferLength ) {
		this(transferLength, 10, 10, null, "S3 Transfer");
	}
	
	public void reset(long newLength) {
		length = newLength;
		lastPct = 0;
		lastTime = System.currentTimeMillis();
		bytes = 0;
	}
	
	public void setIncrements( float pctInc, float timeIncSec ) {
		pctinc = pctInc;
		timeinc = (long) (1000*timeIncSec);
	}
	
	public void progressChanged(ProgressEvent progressEvent) {
		ProgressEventType type = progressEvent.getEventType();
		//System.out.println(type.toString());
		//logger.log(Level.FINEST,type.name());
		if (type == ProgressEventType.REQUEST_BYTE_TRANSFER_EVENT || type == ProgressEventType.RESPONSE_BYTE_TRANSFER_EVENT) {
			bytes += progressEvent.getBytes();
			pct = Math.min(99.999f, 100f*bytes/length);
			time = System.currentTimeMillis();
			if (time-lastTime > timeinc || pct-lastPct > pctinc) {
				logger.log(level,prefix + ": " + bytes + " bytes transferred " + pct + " pct complete " );
				lastTime = time;
				lastPct = pct;
			}
		}
		
	}

}
