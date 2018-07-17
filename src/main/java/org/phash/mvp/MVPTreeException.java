package org.phash.mvp;

/** MVPTreeException
 *  Exceptions related to creation or operations on mvptree data structure
 *  @author dgs
 *  @version 0.1
 **/
public class MVPTreeException extends RuntimeException {
	MVPTreeException(){
		super();
	}
	MVPTreeException(String msg){
		super(msg);
	}
	MVPTreeException(String msg, Throwable cause){
		super(msg,cause);
	}
	MVPTreeException(String msg, Throwable cause, boolean enableSuppression, boolean writeableStackTrace){
		super(msg, cause, enableSuppression, writeableStackTrace);
	}
	MVPTreeException(Throwable cause){
		super(cause);
	}
}
