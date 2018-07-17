package org.phash.mvp;

/**
 * DataPointException 
 * DataPoint persistence and retrieval related exceptions.
 * @author dgs
 * @version 0.1
 **/
public class DataPointException extends RuntimeException {
	DataPointException(){
		super();
	}
	DataPointException(String msg){
		super(msg);
	}
	DataPointException(String msg, Throwable cause){
		super(msg,cause);
	}
	DataPointException(String msg, Throwable cause, boolean enableSuppression, boolean writeableStackTrace){
		super(msg, cause, enableSuppression, writeableStackTrace);
	}
	DataPointException(Throwable cause){
		super(cause);
	}
}
