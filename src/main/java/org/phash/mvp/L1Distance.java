package org.phash.mvp;

/**
 * L1 Metric Distance 
 *
 * @author dgs   
 * @version 0.1
 **/

public class L1Distance<T extends Number> implements MetricDistance<T>{

	public Double distance(DataObject<T> obj1, DataObject<T> obj2){
		T[] x = obj1.getDataWithoutTx();
		T[] y = obj2.getDataWithoutTx();
		if (x.length != y.length)
			throw new DataPointException("unequal data arrays in DataObjects");
		Double sum = 0.0;
		for (int i=0;i<x.length;i++){
			sum = sum + Math.abs(x[i].doubleValue() - y[i].doubleValue());
		}
		return sum/x.length;
	}
}
