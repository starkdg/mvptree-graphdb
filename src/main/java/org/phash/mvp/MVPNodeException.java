package org.phash.mvp;

/** MVPNodeException 
 *  Exceptions related to creation or retrieval of mvp nodes
 *  @author dgs
 *  @version 0.1
 **/
public class MVPNodeException extends RuntimeException {
	MVPNodeException(){
		super();
	}
	MVPNodeException(String msg){
		super(msg);
	}
	MVPNodeException(String msg, Throwable cause){
		super(msg,cause);
	}
	MVPNodeException(String msg, Throwable cause, boolean enableSuppression, boolean writeableStackTrace){
		super(msg, cause, enableSuppression, writeableStackTrace);
	}
	MVPNodeException(Throwable cause){
		super(cause);
	}
}
