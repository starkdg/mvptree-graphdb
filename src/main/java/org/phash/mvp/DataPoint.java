package org.phash.mvp;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.DynamicLabel;
import org.apache.commons.lang3.ArrayUtils;
import java.util.Arrays;

/**
 * <h1> DataPoint </h1>
 * The object which manages persistence and access
 * to a unit of data to be stored in the mvptree along with
 * all its relevant properties, such as id string, 
 * an active flag, and its corresponding data array of generic
 * type, T[].  It also provides a metric distance function to 
 * compute the distance between two DataPoint's or between a 
 * DataPoint and a TargetPoint.
 *
 * @author dgs
 * @version 0.1 
 */

public class DataPoint<T extends Number> implements DataObject<T>{

	private final String ActiveProperty = "ACTIVE";
	private final String IdProperty     = "ID";
	protected final String DataProperty = "DATA";
	protected final String DataPointLabel = "DATAPOINT";

	/**
	 * A path of distances between this DataPoint and each vantage point in leaf node.
	 **/
	private float[] path; 
	private final Class<T> type;
	private final Node node;
	static private int num_distance_ops = 0;

	/**
	 *    Constructor
	 * @param Node    neo4j node
	 * @param int     pl, pathlength for float[] path variable
	 **/
	protected DataPoint(Node node, int pl, Class<T> type){
		if (node == null)
			throw new NullPointerException("node arg is null");
		if (pl <= 0)
			throw new IllegalArgumentException("pl arg value not valid");
		this.node = node;
		this.path = new float[pl];
		this.type = type;
	}

	/**
	 *   Constructor
	 * @param Node      neo4j node
	 * @param float[]   path
	 * @param Class<T>  type of generic type (necessary to get type at runtime)
	 **/
	protected DataPoint(Node node, float[] path, Class<T> type){
		if (node == null || path == null)
			throw new NullPointerException("one or both params null");
		this.node = node;
		this.path = path;
		this.type = type;
	}

	/** 
	 *  Label the node as a DataPoint in neo4j db
	 **/
	protected void setLabel(){
		node.addLabel(DynamicLabel.label(DataPointLabel));
	}

	/**
	 * Reset counter for number of distance() operations to zero.
	 * @return void
	 */
	static public void reset(){
		DataPoint.num_distance_ops = 0;
	}

	/**
	 * Increment variable to count number of distance calculations
	 **/
	static protected void incr(){
		DataPoint.num_distance_ops++;
	}

	/**
	 * Get count of number for distance operations
	 * @return int 
	 */
	static public int getNumOpsCount(){
		return DataPoint.num_distance_ops;
	}

	/**
	 * Get neo4j node object
	 * @return Node
	 **/
	protected Node getNode(){
		return node;
	}

	/**
	 * Mark DataPoint as currently active/inactive
	 * @param boolean   activeFlag
     * @return void
	 **/
	protected void setActive(boolean activeFlag){
		getNode().setProperty(ActiveProperty, activeFlag);
	}

	/** Get the DataPoint's status in tree.
	 * @return boolean 
	 */
	protected boolean isActive(){
		boolean	activeflag = (boolean)getNode().getProperty(ActiveProperty);
		return activeflag;
	}

	/**
	 * Set the id string of DataPoint.
	 * @param String   id
	 * @return void
	 */
	public void setId(String id){
		try (Transaction tx = getNode().getGraphDatabase().beginTx()){
			getNode().setProperty(IdProperty, id);
			tx.success();
		}
	}

	/** Set the id string of DataPoint without transactional support (internal use)
	 *  @param String    id
	 *  @return void
	 **/
	protected void setIdWithoutTx(String id){
		getNode().setProperty(IdProperty, id);
	}

	/**
	 * Get the id string of datapoint
	 * @return String
	 */
	public String getId(){
		String id;
		try (Transaction tx = getNode().getGraphDatabase().beginTx()){
			id = (String)getNode().getProperty(IdProperty);
			tx.success();
		}
		return id;
	}

	/** Get id string of DataPoint without neo4j transactional support (internal use)
	 * @return String
	 **/
	protected String getIdWithoutTx(){
		String id = (String)getNode().getProperty(IdProperty);
		return id;
	}

	/** Add a distance to its path at index position
	 * @param int     index
	 * @param float   d
	 * @return void
	 **/
	protected void addToPath(int index, float d){
		if (index >= 0 && index < path.length){
			path[index] = d;
		}
	}


	/** Get a distance in its path at position, n
	 * @param int   n
	 * @return float
	 **/
	protected float getPathDistance(int n){
		return path[n];
	}

	/** Set the entire path.
	 *  @param float[]   pathdistances
	 *  @return void
	 **/
	protected void setPath(float[] pathdistances){
		this.path = pathdistances;
	}

	/** Get path of distances.
	 * @return float[]
	 **/
	protected float[] getPath(){
		return path;
	}

	/** Delete the neo4j node in db.
	 **/
	protected void delete(){
		Iterable<Relationship> rels = getNode().getRelationships();
		for (Relationship rel : rels){
			rel.delete();
		}
		node.delete();
	}

	/** Is the same point as another point.
	 * @param DataPoint<>   point
	 * @return boolean
	 **/
	protected boolean isSamePoint(DataPoint<?> point){
		if (getNode().getId() == point.getNode().getId())
			return true;
		return false;
	}


	/** Set data array for DataPoint.
	 *  @param T[]   data
	 *  @return void
	 **/
	public void setData(T[] data){
		try (Transaction tx = getNode().getGraphDatabase().beginTx()){
			setDataWithoutTx(data);
			tx.success();
		} catch (Exception ex){
			throw new DataPointException(ex);
		}
	}

	/** Set data array for DataPoint without transactional support.
	 *  @param T[] data
	 *  @return void
	 **/
	protected void setDataWithoutTx(T[] data){
		if (type == Float.class){
			float[] primarray = ArrayUtils.toPrimitive((Float[])data);
			getNode().setProperty(DataProperty, primarray);;
		} else if (type == Double.class){
			double[] primarray = ArrayUtils.toPrimitive((Double[])data);
			getNode().setProperty(DataProperty, primarray);
		} else if (type == Integer.class) {
			int[] primarray = ArrayUtils.toPrimitive((Integer[])data);
			getNode().setProperty(DataProperty, primarray);;
		} else if (type == Byte.class) {
			byte[] primarray = ArrayUtils.toPrimitive((Byte[])data);
			getNode().setProperty(DataProperty, primarray);
		} else if (type == Long.class) {
			long[] primarray = ArrayUtils.toPrimitive((Long[])data);
			getNode().setProperty(DataProperty, primarray);
		} else
			throw new DataPointException("datatype not float, double, int, byte, long");
	}


	/** Get data array
	 * @return T[] 
	 **/
	public T[] getData(){
		T[] result = null;
		try (Transaction tx = getNode().getGraphDatabase().beginTx()){
			result = getDataWithoutTx();
			tx.success();
		} catch (Exception ex){
			throw new DataPointException(ex);
		}
		return result;
	}

	
	/** Get data array without transactional support (internal use)
	 *  @return T[]
	 **/
	public T[] getDataWithoutTx(){
		T[] result = null;
		if (type == Float.class){
			float[] primarray = (float[])(getNode().getProperty(DataProperty));
			result = (T[])ArrayUtils.toObject(primarray);
		} else if (type == Double.class){
			double[] primarray = (double[])(getNode().getProperty(DataProperty));
			result = (T[])ArrayUtils.toObject(primarray);
		} else if (type == Integer.class){
			int[] primarray = (int[])(getNode().getProperty(DataProperty));
			result = (T[])ArrayUtils.toObject(primarray);
		} else if (type == Byte.class) {
			byte[] primarray = (byte[])(getNode().getProperty(DataProperty));
			result = (T[])ArrayUtils.toObject(primarray);
		} else if (type == Long.class) {
			long[] primarray = (long[])(getNode().getProperty(DataProperty));
			result = (T[])ArrayUtils.toObject(primarray);
		} else
			throw new DataPointException("datatype not float, double, int, byte, long");

		return result;
	}
}


