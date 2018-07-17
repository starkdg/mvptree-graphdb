package org.phash.mvp;

/**
 * <h1>TargetPoint</hh>
 * TargetPoint is Used for querying the tree for a given
 * DataPoint. It does not create any  additional 
 * nodes in the database. 
 * @author dgs
 * @version 0.1
 */
public class TargetPoint<T extends Number> implements DataObject<T> {

	T[] data;

	/** constructor **/
	public TargetPoint(){};

	/** consturctor 
	 * @param T[]   data
	 **/
	public TargetPoint(T[] data){
		this.data = data;
	}

	/** Set the data.
	 *  @param T[]   data
	 *  @return void
	 **/
	public void setData(T[] data){
		this.data = data;
	}

	/** Get the Data 
	 *  @return T[]
	 **/
	public T[] getData(){
		return data;
	}

	/** Get the data 
	 * (identical to getData() but necessary for symmetry with DataPoint class)
	 * @return T[]
	 **/
	public T[] getDataWithoutTx(){
		return data;
	}
}
