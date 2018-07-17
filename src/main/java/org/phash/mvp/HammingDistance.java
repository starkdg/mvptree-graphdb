package org.phash.mvp;

/**
 * Hamming Metric Distance 
 *
 * @author dgs   
 * @version 0.1
 **/

public class HammingDistance<T extends Number> implements MetricDistance<T>{

	public Double distance(DataObject<T> obj1, DataObject<T> obj2){
		T[] x = obj1.getDataWithoutTx();
		T[] y = obj2.getDataWithoutTx();
		if (x.length != y.length)
			throw new DataPointException("unequal data arrays in DataObjects");
		int nbits = 0;
		for (int i=0;i<x.length;i++){
			long xord = x[i].longValue() ^ y[i].longValue();
			nbits += Long.bitCount(xord);
		}
		return (double)nbits;
	}
}
