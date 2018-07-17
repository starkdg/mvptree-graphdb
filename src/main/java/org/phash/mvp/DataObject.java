package org.phash.mvp;

/**
 * Interface for both DataPoint and TargetPoint objects 
 *
 * @author dgs   
 * @version 0.1
 **/

public interface DataObject<T extends Number>{

	public T[] getData();

	public T[] getDataWithoutTx();
	
	public void setData(T[] data);
}
