package org.phash.mvp;

/**
 * Interface for Metric distance implementation 
 *
 * @author dgs   
 * @version 0.1
 **/

public interface MetricDistance<T extends Number>{

	public Double distance(DataObject<T> obj1, DataObject<T> obj2);;

}
