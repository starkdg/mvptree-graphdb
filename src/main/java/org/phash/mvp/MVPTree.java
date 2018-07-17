package org.phash.mvp;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Vector;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Enumeration;
import java.io.PrintStream;

import org.neo4j.graphdb.Transaction;

enum ComparatorType {
	LESS_THAN, GREATER_THAN
}

/** <h1>MVPTree</h1>
 * @author dgs
 * @version 0.1
 */
public class MVPTree<T extends Number> {

	private final NodeFactory<T> nf;
	private final Class<T> type;
	private final MetricDistance<T> metric;

	/** Constructor
	 * Uses default values for:
	 *    branch factor, bf = 2
	 *    path length, pl = 8
	 *    leaf min, lm = 30
	 *    no. levels per node, nl = 2
	 * @param String graphdb directory
	 * @param String neo4j properties file
	 * @param Class<T> Class of generic type (necessary to determine at runtime)
	 **/
	public MVPTree(String graphdbdir, String propsfile, Class<T> type){
		this.metric = new L1Distance<>();
		this.type = type;
		int default_bf = 2;
		int default_pl = 8;
		int default_lm = 10;
		int default_nl = 2;
		this.nf = new NodeFactory<T>(graphdbdir, propsfile, default_bf,
									 default_pl, default_lm, default_nl, type);
	}

	/**
	 * Constructor
	 * @param String    neo4j graph db directory
	 * @param String    neo4j properties file
	 * @param int       bf, branch factor
	 * @param int       pl, path length
	 * @param int       lm, leaf minimum
	 * @param int       nl, number levels per node
	 * @param DistanceFunction  enumerated value of distance type
	 * @param Class<T> Class of generic type (necessary to determine at runtime)
	 */
	public MVPTree(String graphdbdir, String propsfile, int bf, int pl, int lm, int nl,
				   DistanceFunction distance_type, Class<T> type){
		this.type = type;

		switch (distance_type) {
		case L1:
			this.metric = new L1Distance<>();
			break;
		case L2:
			this.metric = new L2Distance<>();
			break;
		case HAMMING:
			this.metric = new HammingDistance<>();
			break;
		default:
			throw new MVPTreeException("no such distance metric");
		}
		
		this.nf = new NodeFactory<T>(graphdbdir, propsfile, bf, pl, lm, nl, type);
	}

	/**
	 * Constructor
	 * @param String graph db directory
	 * @param String neo4j properties file
	 * @param int    branchfactor, bf (e.g. 2, 3)
	 * @param int    pathlength, pl   (e.g. 4, 8, ...)
     * @param int    leaf minimum, lm (e.g. 10)
     * @param int    no. levels per node (e.g. 2, 4)
	 * @param MetricDistance     custom metric distance implementation
	 * @param Class<T> type  Class of generic type (necessary to determine at runtime)
	 **/
	public MVPTree(String graphdbdir, String propsfile, int bf, int pl, int lm, int nl,
				   MetricDistance<T> metric, Class<T> type){
		if (metric == null)
			throw new NullPointerException("metric distance object is null");
		
		this.type = type;
		this.metric = metric;
		this.nf = new NodeFactory<T>(graphdbdir, propsfile, bf, pl, lm, nl, type);
	}

	/**
	 * Initialize graph database. This only needs to be called, if
	 * the graphdb was purposefully shutdown by calling
	 *  shutdown().
	 */
	public void initGraphdb(){
		nf.initGraphDatabase();
	}

	public void shutdown(){
		nf.shutdown();
	}

	/**
	 * get tree parameters
	 **/
	public int getBranchFactor(){return nf.getBranchFactor();}
	public int getPathLength(){return nf.getPathLength();}
	public int getNumLevelsPerNode(){return nf.getNumLevelsPerNode();}
	public int getLeafMinimum(){return nf.getLeafMinimum();}

	/** create a DataPoint in graph database
	 *  setId() and setData() must still be called on DataPoints
	 * @return DataPoint
	 */
	public DataPoint<T> createDataPoint(){
		DataPoint<T> pnt = null;
		try (Transaction tx = nf.getGraphdb().beginTx()){
			pnt = nf.createDataPoint();
			tx.success();
		} catch (Exception ex){
			throw new DataPointException("unable to create data point", ex);
		}
		return pnt;
	}

	/** Create a list of DataPoints
	 * setId() and setData() must still be called on each one before
	 * adding to tree.
	 * @param int     number of points to create
	 * @return ArrayList<DataPoint<T>>
	 */
	public ArrayList<DataPoint<T>> createDataPoints(int n){
		ArrayList<DataPoint<T>> points = null;
		try (Transaction tx = nf.getGraphdb().beginTx()){
			points = nf.createDataPoints(n);
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to create data points", ex);
		}
		return points;
	}

	/** Calculate distance between two DataPoints or between a
	 *  DataPoint and a TargetPoint
	 * @param DataPoint<T>  one of two points
	 * @param DataObject<T> second of two points
	 * @return Double
	 */
	public Double distanceBetweenPoints(DataPoint<T> pntA, DataObject<T> pntB){
		Double result = 0.0;
		try (Transaction tx = nf.getGraphdb().beginTx()){
			result = metric.distance(pntA, pntB);
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to calculate distance", ex);
		}
		return result;
	}


	/** Get total number of points indexed in tree.
	 * @return int
	 */
	public int getDataPointCount(){
		int count = 0;
		try (Transaction tx = nf.getGraphdb().beginTx()){
			count = nf.getCount();
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to count data points in index", ex);
		}
		return count;
	}

	/** Remove a data point from index by its String id
	 *  for points that have been added to tree.
	 *  Does not delete the node, but removes it from the
	 *  index and marks it inactive for a later deletion.
	 *  @param String     id
	 *  @return void
	 */
	public void removePoint(String id){
		try (Transaction tx = nf.getGraphdb().beginTx()){
			nf.deleteDataPoint(id);
			tx.success();
		} catch (Exception ex) {
			throw new MVPTreeException("unable to remove point", ex);
		}
	}

	/** Delete a DataPoint.
	 *  Method does not remove a point from the tree. Only to be
	 *  used for newly created datapoint not yet added to tree.
	 *  @param DataPoint<T>  pnt
	 *  @return void
	 */
	public void deletePoint(DataPoint<T> pnt){
		try (Transaction tx = nf.getGraphdb().beginTx()){
			pnt.delete();
			tx.success();
		}
	}

	/** Retrieve DataPoint by its String id.
	 * @param  String   id
	 * @return DataPoint<T>
	 **/
	public DataPoint<T> lookup(String id){
		DataPoint<T> pnt = null;
		try (Transaction tx = nf.getGraphdb().beginTx()){
			pnt = nf.lookupDataPoint(id);
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to lookup point", ex);
		}
		return pnt;
	}


	/* Calculate distances of all DataPoints from given vantage point, vp. */
	/* Called when collating points for internal node */
	private ArrayList<Float> calcPointDistances(DataPoint<T> vp,
												ArrayList<DataPoint<T>> points){
		ArrayList<Float> dists = null;
		if (points != null && points.size() > 0){
			dists = new ArrayList<Float>(points.size());
			for (int i = 0;i < points.size();i++){
				Double d = metric.distance(vp, points.get(i));
				if (d.floatValue() < 0.0 || d.isNaN())
					throw new DataPointException("bad distance calculation: " + d);
				dists.add(d.floatValue());
			}
		}
		return dists;
	}

	/* Calculate distances of all DataPoints from given vantage point, vp.
       Mark distances in path[level] of each DataPoint. Called when adding
       points to a leaf node. */
	private void markPointDistancesOnPath(DataPoint<T> vp,
										  ArrayList<DataPoint<T>> points,
										  int level){
		if (points != null && points.size() > 0){
			for (int i = 0;i < points.size();i++){
				DataPoint<T> pnt = points.get(i);
				Double d = metric.distance(vp, pnt);
				if (d < 0 || d.isNaN())
					throw new DataPointException("bad distance value: " + d);
				pnt.addToPath(level, d.floatValue());
			}
		}
	}

	/* Calculate split points that divide the points into slices based on their 
     *  distances from a vantage points.  Calculated  to split the points into bf 
     *  number of partitions  based on equal number of points to each partition.
	 *  Note: this still does not guarantee a perfectly balanced tree, since it is 
     *  based on the current set of points, and there is no way to know the 
     *  distribution of subsequent points added to the tree.
	 *@param ArrayList<Float>  array of distances
	 *@param float[]           splits (out)
	 *@param int              split_index position in splits array
	 *@return void
	*/
	private void calcSplitPoints(ArrayList<Float> dists, float[] splits, int split_index){
		//split dataset in equal portions
		int bf = nf.getBranchFactor();
		int lengthM = bf-1;
		if (dists.size() > 0){
			if (splits[split_index*lengthM] == -1){
				float[] tmpdists = new float[dists.size()];
				for (int i=0;i<tmpdists.length;i++){
					tmpdists[i] = dists.get(i);
				}
				Arrays.sort(tmpdists);
				float factor = (float)tmpdists.length/(float)bf;
				for (int i=0;i < lengthM;i++){
					float pos = (float)(i+1)*factor;
					int hi = (pos <= tmpdists.length-1) ? (int)Math.ceil(pos) : 0;
					int lo = (int)Math.floor(pos);
					float slice = (tmpdists[lo] + tmpdists[hi])/2.0f;
					splits[split_index*lengthM + i] = slice;
				}
			}
		}
	}

	/* Alternate function to divide space within internal node 
     * by maximum/minimum distance values instead of by equal number
     * of points.  
	 */
	private void calcSplitPoints2(ArrayList<Float> dists, float[] splits, int split_index){
		//split points according to median
		int bf = nf.getBranchFactor();
		int lengthM = bf-1;
		if (dists.size() > 0){
			if (splits[split_index*lengthM] == -1){
				float minf = Float.MAX_VALUE;
				float maxf = Float.MIN_VALUE;
				int N = dists.size();
				for (int i=0;i<N;i++){
					if (dists.get(i) < minf) minf = dists.get(i);
					if (dists.get(i) > maxf) maxf = dists.get(i);
				}
				float slice = (maxf + minf)/bf;
				for (int i=0;i < lengthM;i++){
					splits[split_index*lengthM + i] = (i+1)*slice;
				}
			}
		}
	}

	/* Compare value to split according to ComparatorType enum (less than or greater than)
	 *@param float          value
     *@param float          split
	 *@param ComparatorType comp
     *@return boolean
	 **/
	private boolean compareDistanceToSplitPoint(float value, float split, ComparatorType comp){
		if (comp == ComparatorType.LESS_THAN)
			return (value <= split);
		return (value > split);
	}

	/* Cull points from list that satisfy a particular relation from a split value.
	 * @param ArrayList<DataPoint<T>>    points to compare to split value
	 * @param ArrayList<Float>           dists, distances of each point to a particular vp.
	 * @param float                      split value.
	 * @param enum ComparatorType        less than/greater than
	 * @return ArrayList<DataPoint>  
	 */
	private ArrayList<DataPoint<T>> cullPoints(ArrayList<DataPoint<T>> points,
											   ArrayList<Float> dists,
											   float split,
											   ComparatorType comp){
		ListIterator<DataPoint<T>> iter = points.listIterator();
		ListIterator<Float> iter_dists = dists.listIterator();
		ArrayList<DataPoint<T>> culledlist = null;
		while (iter.hasNext()){
			float d = iter_dists.next();
			DataPoint<T> pnt = iter.next();
			if (compareDistanceToSplitPoint(d, split, comp)){
				if (culledlist == null) culledlist = new ArrayList<DataPoint<T>>();
				culledlist.add(pnt);
				iter.remove();
				iter_dists.remove();
			}
		}
		return culledlist;
	}

	// expand node by getting its child nodes (if it is internal node) and adding them
	// to hashtable of childnodes with Integer Key that represents its position within
	// next layer of tree.
	// @param index - int value for position of node in current layer of tree.  
    private void expandNode(MVPNode<T> node,
							Hashtable<Integer, MVPNode<T>> childNodes, int index){
		int fanout = (int)Math.pow(nf.getBranchFactor(), nf.getNumLevelsPerNode());
		if (node != null && node instanceof MVPInternal){
			MVPInternal<T> internal = (MVPInternal<T>)node;
			for (int i=0;i<fanout;i++){
				MVPNode<T> child = internal.getChildNode(i);
				if (child != null) childNodes.put(index*fanout+i, child);
			}
		}
	}

	/* Collate points for an internal node by adding each point in points
	 *  to an ArrayList in childpoints.
	 * @param MVPInternal           mvpnode to which points are being added.
	 * @param ArrayList<DataPoint>  list of DataPoints to collate
	 * @param HashTable<Integer, ArrayList<DataPoint<T>>  hash of datapoint lists
	 * @param index int index of current node being processed at a current level.
	 */
    private void collatePoints(MVPInternal<T> internalNode,
							   ArrayList<DataPoint<T>> points, 
                               Hashtable<Integer,ArrayList<DataPoint<T>>> childpoints, 
							   int index){
	
		Hashtable<Integer, ArrayList<DataPoint<T>>> pnts = new Hashtable<>();
		pnts.put(0, points);

		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int lengthM = bf - 1;
		int fanout = (int)Math.pow(bf, nl);
		int n=0;
		do {
			int nbnodes = (int)Math.pow(bf,n);
			int nbchildnodes = (int)Math.pow(bf, n+1);
			Hashtable<Integer,ArrayList<DataPoint<T>>> pnts2 = new Hashtable<>();
			
			int lengthMn = (lengthM)*(int)Math.pow(bf, n);
			float[] msplits = internalNode.getSplitsAtLevel(n);
			if (msplits == null) {
				msplits = new float[lengthMn];
				Arrays.fill(msplits, -1.0f);// mark -1.0 as sentinal (not calculated yet)
			}

			DataPoint<T> vp = internalNode.getVantagePoint(n, nf);

			for (Enumeration<Integer> e = pnts.keys();e.hasMoreElements();){
				int nodeIndex = e.nextElement();
				ArrayList<DataPoint<T>> list = pnts.get(nodeIndex);
				ArrayList<Float> dists = calcPointDistances(vp, list);
				if (dists != null && dists.size() > 0){
					calcSplitPoints(dists, msplits, nodeIndex);
					float m;
					for (int j=0;j<lengthM;j++){
						m = msplits[nodeIndex*lengthM+j];
						ArrayList<DataPoint<T>> culledpts = cullPoints(
								   list, dists,m,ComparatorType.LESS_THAN);
						if (culledpts != null) pnts2.put(nodeIndex*bf+j, culledpts);
					}
					m = msplits[nodeIndex*lengthM+lengthM-1];
					ArrayList<DataPoint<T>> culledpts = cullPoints(
								list, dists, m, ComparatorType.GREATER_THAN);
					if (culledpts != null) pnts2.put(nodeIndex*bf+bf-1, culledpts);
				}
				if (list != null && list.size() > 0){
					StringBuilder msg = new StringBuilder();
					msg.append("list " + nodeIndex + " n = " + n);
					msg.append(" not fully collated, " + list.size() + " points remaining ");
					throw new MVPTreeException(msg.toString());
				}
			}
			internalNode.setSplitsAtLevel(msplits, n);
			pnts = pnts2;
			n++;
		} while (n < nl);
	
		for (int i=0;i<fanout;i++){
			ArrayList<DataPoint<T>> childlist = pnts.get(i);
			if (childlist != null) childpoints.put(index*fanout+i, childlist);
		}
    }

	/* Link nodes in one layer of tree to next layer (nodes --> childnodes), 
	 * if the node is not null and is an internal node.
	 * @param HashTable<Integer,MVPNode> nodes   list of nodes in layer, level-nl
	 * @param HashTable,Integer,MVPNode> childnodes list of nodes in layer, level
	 * @param int          current child level
	*/
    private void linkNodes(Hashtable<Integer,MVPNode<T>> nodes,
						   Hashtable<Integer, MVPNode<T>> childnodes,
						   int level){
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int fanout = (int)Math.pow(bf, nl);
		int nbnodes = (int)Math.pow(bf, level-nl);
		int nbchildnodes = (int)Math.pow(bf, level);

		for (Enumeration<Integer> e = nodes.keys();e.hasMoreElements();){
			int i = e.nextElement();
			MVPNode<T> mvpnode = nodes.get(i);
			if (mvpnode instanceof MVPInternal){
				MVPInternal<T> internal = (MVPInternal<T>)mvpnode;
				for (int j=0;j<fanout;j++){
					MVPNode<T> childnode = childnodes.get(i*fanout+j);
					if (childnode != null) internal.setChildNodeAt(childnode, j);
				}
			}
		}
	}

	/* process a current list at a particular node position in tree (level and node index),
	 * creating nodes as needed, or adding points to a particular node.  
	 * Process a current list at a particular node position in tree (level, node index).
	 * Create nodes as need, add points to leaf nodes.
	 */
	private MVPNode<T> processNode(int level, int index, MVPNode<T> node,
								   ArrayList<DataPoint<T>> points, 
								   Hashtable<Integer, MVPNode<T>> childNodes,
								   Hashtable<Integer,ArrayList<DataPoint<T>>> childpoints){
		if (points == null)
			throw new MVPTreeException("null list");
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int lm = nf.getLeafMinimum();
		int pl = nf.getPathLength();
		int leaf_limit = (int)Math.pow(bf,nl)*lm;
		MVPNode<T> retnode = node;
		if (node == null){        //create node 
			if (points.size() >= leaf_limit){
				MVPInternal<T> internal = nf.createInternalNode(points);
				collatePoints(internal, points, childpoints, index);
				retnode = internal;
			} else if (points.size() > 0){                  // create leaf node
				MVPLeaf<T> leaf = nf.createLeafNode(points);
				if (points.size() > 0){
					for (int i = 0;i < pl;i++){
						DataPoint<T> vp = leaf.getVantagePoint(i, nf);
						markPointDistancesOnPath(vp, points, i);
					}
					leaf.addDataPoints(points);
				}
				retnode = leaf;
			}
		} else { // node exists
			if (MVPInternal.class.isInstance(node)){ // internal node
				collatePoints((MVPInternal<T>)node, points, childpoints, index);
			} else if (MVPLeaf.class.isInstance(node)){  // leaf node
				MVPLeaf<T> leaf = (MVPLeaf<T>)node;
				int numvps = leaf.getNumVantagePoints();
				int nbpoints = leaf.getDataPointCount();
				if (nbpoints + points.size() >= leaf_limit){
					ArrayList<DataPoint<T>> vps = leaf.getVantagePoints(numvps, nf);
					ArrayList<DataPoint<T>> existing_pnts = leaf.getAllDataPoints(nf);
					points.addAll(existing_pnts);
					points.addAll(vps);
					MVPInternal<T> internal = nf.createInternalNode(points);
					collatePoints(internal, points, childpoints, index);
					leaf.delete();
					retnode = internal;
				} else {
					leaf.selectVantagePoints(points, numvps, pl);
					if (points.size() > 0){
						for (int i=0;i<pl;i++){
							DataPoint<T> vp = leaf.getVantagePoint(i, nf);
							markPointDistancesOnPath(vp, points, i);
						}
						leaf.addDataPoints(points);
					}
					retnode = leaf;
				}
			} else {
				throw new MVPNodeException("no mvpnode at level " + level + "index " + index);
			}
		}
		expandNode(retnode, childNodes, index);
		return retnode;
    }

	/** Add points to tree. 
	 * @param ArrayList<DataPoint<T>>  points
	 * @return void
	 * @throws MVPTreeException 
	 */
	public synchronized void addPoints(ArrayList<DataPoint<T>> points){
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();

		try (Transaction tx = nf.getGraphdb().beginTx()){
			MVPNode<T> topnode = nf.getTopNode();
			Hashtable<Integer, MVPNode<T>> prevnodes = null;
			Hashtable<Integer, MVPNode<T>> currentnodes = new Hashtable<Integer, MVPNode<T>>(1);
			if (topnode != null) currentnodes.put(0, topnode);
			
			Hashtable<Integer, ArrayList<DataPoint<T>>> pnts = new Hashtable<>(1);
			pnts.put(0,points);
			
			int n = 0;
			int fanout = (int)Math.pow(bf, nl);
			boolean done = true;
			
			nf.indexPoints(points);
			do {
				done = true;
				int nbnodes = (int)Math.pow(bf, n);
				int nbchildnodes = (int)Math.pow(bf, n+nl);
				Hashtable<Integer, MVPNode<T>> childNodes = new Hashtable<>();
				Hashtable<Integer, ArrayList<DataPoint<T>>> pnts2 = new Hashtable<>();
				
				for (Enumeration<Integer> e = pnts.keys();e.hasMoreElements();){
					int index = e.nextElement();
					MVPNode<T> newnode = null, mvpnode = currentnodes.get(index);
					ArrayList<DataPoint<T>> list = pnts.get(index);
					newnode = processNode(n, index, mvpnode, list, childNodes, pnts2);
					if (newnode != null && !newnode.isSameAs(mvpnode)){
						currentnodes.put(index, newnode);
						if (n == 0)	newnode.setAsTop(nf);
					}
				}
				if (prevnodes != null) linkNodes(prevnodes, currentnodes, n);
				prevnodes = currentnodes;
				currentnodes = childNodes;
				pnts = pnts2;
				n += nf.getNumLevelsPerNode();
				if (!pnts2.isEmpty()) done = false;
			} while (!done);
			nf.saveParameters();
			tx.success();
		} catch (Exception ex) {
			throw new MVPTreeException("unable to add points", ex);
		}
	}

	/**
	 * Calculate stats for tree. .
	 * @param MVPTreeStats 
	 * @return void
	 * @throws MVPTreeException
	 */
	public synchronized void stats(MVPTreeStats stats){
		int bf = nf.getBranchFactor();
		int lm = nf.getLeafMinimum();
		int nl = nf.getNumLevelsPerNode();

		/* init */
		stats.n_total_points = 0;
		stats.n_vps = 0;
		stats.n_points = 0;
		stats.n_internal = 0;
		stats.n_leaf = 0;
		stats.n_fringe_nodes = 0;
		stats.depth = 0;
		stats.max_leaf_size = 0;
		stats.min_leaf_size = 1000000;
		stats.avg_leaf_size = 0.0f;
		
		try (Transaction tx = nf.getGraphdb().beginTx()){
			MVPNode<T> topnode = nf.getTopNode();
			Hashtable<Integer,MVPNode<T>> currentnodes = new Hashtable<>(1);
			if (topnode != null) currentnodes.put(0,topnode);
			stats.n_total_points = nf.getCount();

			int n = 0, depth = 0;
			int fanout = (int)Math.pow(nf.getBranchFactor(), nf.getNumLevelsPerNode());
			boolean done;
			do {
				done = true;
				int nbnodes = (int)Math.pow(bf, n);
				int nbchildnodes = (int)Math.pow(bf,n+nl);
				Hashtable<Integer,MVPNode<T>> childnodes = new Hashtable<>();
				
				for (Enumeration<Integer> e = currentnodes.keys();e.hasMoreElements();){
					int index = e.nextElement();
					MVPNode<T> mvpnode = currentnodes.get(index);
					int numvps = mvpnode.getNumVantagePoints();
					stats.n_vps += numvps;
					if (MVPInternal.class.isInstance(mvpnode)){
						stats.n_internal++;
					} else if (MVPLeaf.class.isInstance(mvpnode)){
						MVPLeaf<T> leaf = (MVPLeaf<T>)mvpnode;
						int nbpoints = leaf.getDataPointCount();
						if (nbpoints < stats.min_leaf_size)
							stats.min_leaf_size = nbpoints;
						if (nbpoints > stats.max_leaf_size)
							stats.max_leaf_size = nbpoints;
						stats.n_points += nbpoints;
						stats.n_leaf++;
					}
					expandNode(mvpnode, childnodes, index);
				}
				stats.depth++;
				stats.n_fringe_nodes = currentnodes.size();
				n += nl;
				if (!childnodes.isEmpty()){
					done = false;
					currentnodes = childnodes;
				}
			} while (!done);
			stats.avg_leaf_size = (float)stats.n_points/(float)stats.n_leaf;
			tx.success();
		} catch (Exception ex) {
			throw new MVPTreeException("unable to stat tree", ex);
		}
	}

	/**
	 * Print a tree to a print stream.(useful for debugging)
	 * @param PrintStream
	 * @return void
	 * @throws MVPTreeException
	 */
	public synchronized void printTree(PrintStream stream){
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int fanout = (int)Math.pow(bf, nl);

		try (Transaction tx = nf.getGraphdb().beginTx()){
			MVPNode<T> topnode = nf.getTopNode();
			Hashtable<Integer,MVPNode<T>> currentnodes = new Hashtable<>(1);
			if (topnode != null)
				currentnodes.put(0,topnode);
			if (topnode == null)
				stream.printf("Tree is empty.\n");
			stream.printf("Tree structure:\n");
			stream.printf("branch factor: %d\n", nf.getBranchFactor());
			stream.printf("path length: %d\n", nf.getPathLength());
			stream.printf("leaf Minimum: %d\n", nf.getLeafMinimum());
			stream.printf("no. levels per internal node: %d\n", nf.getNumLevelsPerNode());
			stream.printf("total data points: %d\n", nf.getCount());

			int n = 0, depth = 0;
			boolean done;
			do {
				done = true;
				int nbnodes = (int)Math.pow(bf, n);
				int nbchildnodes = (int)Math.pow(bf,n+nl);
				Hashtable<Integer,MVPNode<T>> childnodes = new Hashtable<>();
				
				stream.printf("level = %d\n", n);
				for (Enumeration<Integer> e = currentnodes.keys();e.hasMoreElements();){
					int index = e.nextElement();
					MVPNode<T> mvpnode = currentnodes.get(index);
					int numvps = mvpnode.getNumVantagePoints();
					ArrayList<DataPoint<T>> vps = mvpnode.getVantagePoints(numvps, nf);
					if (MVPInternal.class.isInstance(mvpnode)){
						MVPInternal<T> internal = (MVPInternal<T>)mvpnode;
						stream.printf("  internal(nodeindex %d) \n", index);
						int i = 0;
						for (DataPoint<T> pnt : vps){
							float[] msplits = internal.getSplitsAtLevel(i);
							stream.printf("    (%d) %s splits = ", i++, pnt.getIdWithoutTx());
							for (int j=0;j < msplits.length;j++){
								stream.printf(" %.2f ", msplits[j]);
							}
							stream.printf("\n");
						}
						stream.printf("\n");
					} else if (MVPLeaf.class.isInstance(mvpnode)) {
						MVPLeaf<T> leaf = (MVPLeaf<T>)mvpnode;
						int count = leaf.getDataPointCount();
						stream.printf(" leaf(nodeindex %d)(%d vps) (%d points)\n",
									  index, numvps, count);
						for (DataPoint<T> vp : vps){
							System.out.printf("  vp: %s\n", vp.getIdWithoutTx());
						}
						ArrayList<DataPoint<T>> pnts = leaf.getAllDataPoints(nf);
						for (DataPoint<T> pnt : pnts){
							float[] path = pnt.getPath();
							stream.printf("  %s ", pnt.getIdWithoutTx());
							stream.printf("  path ");
							for (int j=0;j<path.length;j++){
								if (path[j] < 0) break;
								stream.printf(" %.2f, " , path[j]);
							}
							stream.printf("\n");
						}
					}
					expandNode(mvpnode, childnodes, index);
				}
				currentnodes = childnodes;
				n += nf.getNumLevelsPerNode();
				if (!childnodes.isEmpty()) done = false;
			} while (!done);
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to print tree", ex);
		}
	}

	/**
	 * Clear graph database. Use with care.
	 * @return void
	 * @throws MVPTreeException
	 */
	public synchronized void clear(){
		int n = 0, depth = 0;
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int fanout = (int)Math.pow(bf, nl);

		try (Transaction tx = nf.getGraphdb().beginTx()){
			MVPNode<T> topnode = nf.getTopNode();
			Hashtable<Integer,MVPNode<T>> currentnodes = new Hashtable<>(1);
			if (topnode != null) currentnodes.put(0,topnode);
			nf.clearDataPointIndex();
			
			boolean done;
			do {
				done = true;
				int nbnodes = (int)Math.pow(bf,n);
				int nbchildnodes = (int)Math.pow(bf,n+nl);
				Hashtable<Integer,MVPNode<T>> childnodes = new Hashtable<>();
				
				for (Enumeration<Integer> e = currentnodes.keys();e.hasMoreElements();){
					int index = e.nextElement();
					MVPNode<T> mvpnode = currentnodes.get(index);
					int numvps = mvpnode.getNumVantagePoints();
					ArrayList<DataPoint<T>> vps = mvpnode.getVantagePoints(numvps, nf);
					for (DataPoint<T> pnt: vps){
						pnt.delete();
					}
					if (MVPLeaf.class.isInstance(mvpnode)) {
						MVPLeaf<T> leaf = (MVPLeaf<T>)mvpnode;
						ArrayList<DataPoint<T>> pnts = leaf.getAllDataPoints(nf);
						for (DataPoint<T> pnt : pnts){
							pnt.delete();
						}
					}
					expandNode(mvpnode, childnodes, index);
					mvpnode.delete();
				}
				currentnodes = childnodes;
				n += nl;
				if (!childnodes.isEmpty()) done = false;
			} while (!done);
			tx.success();
		} catch (Exception ex){
			throw new MVPTreeException("unable to clear tree", ex);
		}

	}

	private void selectChildNodesToQuery(MVPInternal<T> internal,
										 TargetPoint<T> target,
										 Hashtable<Integer,MVPNode<T>> childnodes,
										 int index,
										 ArrayList<DataPoint<T>> results,
										 float radius){
		int bf = nf.getBranchFactor();
		int lengthM = bf - 1;
		int nl = nf.getNumLevelsPerNode();
		int fanout = (int)Math.pow(bf,nl);
		int n = 0;

		boolean[] current_nodes = { true };
		do {
			int nbnodes = (int)Math.pow(bf, n);
			int nbchildnodes = (int)Math.pow(bf, n+1);
			boolean[] child_nodes = new boolean[nbchildnodes];
			Arrays.fill(child_nodes, false);
			
			DataPoint<T> vp = internal.getVantagePoint(n, nf);
			if (vp == null)
				throw new MVPNodeException("no vantage in internal node " + n);

			Double distance = metric.distance(vp, target);
			if (vp.isActive() && distance.floatValue() <= radius){
				results.add(vp);
			}
			
			int lengthMn = lengthM*nbnodes;
			float[] msplits = internal.getSplitsAtLevel(n);
			if (lengthMn != msplits.length)
				throw new MVPNodeException("inconsistent splits array length");
			
			for (int node_index=0;node_index < nbnodes;node_index++){
				if (current_nodes[node_index]){
					if (msplits[node_index*lengthM] >= 0){
						float m = msplits[node_index*lengthM];
						for (int j=0;j < lengthM;j++){
							m = msplits[node_index*lengthM+j];
							if (distance <= m + radius){
								child_nodes[node_index*bf+j] = true;
							}
						}
						if (distance > m - radius){
							child_nodes[node_index*bf+bf-1] = true;
						}
					}
				}
			}
			current_nodes = child_nodes;
			n++;
		} while (n < nl);
		
		for (int i=0;i < fanout;i++){
			if (current_nodes[i]){
				MVPNode<T> child = internal.getChildNode(i);
				if (child != null){
					childnodes.put(index*fanout+i, child);
				}
			}
		}
	}

	private ArrayList<DataPoint<T>> sortResults(TargetPoint<T> target,
												ArrayList<DataPoint<T>> points){
		ArrayList<DataPoint<T>> results = new ArrayList<>();
		while (points.size() > 0){
			DataPoint<T> pnt = points.remove(0);
			Double d = metric.distance(pnt, target);
			boolean inserted = false;
			for (int i=0;i<results.size();i++){
				if (d.floatValue() <= metric.distance(results.get(i), target)){
					results.add(i, pnt);
					inserted = true;
					break;
				}
			}
			if (!inserted)results.add(pnt);
		}
		return results;
	}

	/* Query tree with a target all DataPoints that 
	 * lie within a given radius.
	 * @param TargetPoint<T> target data
	 * @param float          radius
	 * @return Collection<DataPoint<T>>
	 * @throws MVPTreeException
	 */
	public synchronized Collection<DataPoint<T>> queryTarget(TargetPoint<T> target,
															 float radius){
		int bf = nf.getBranchFactor();
		int nl = nf.getNumLevelsPerNode();
		int fanout = (int)Math.pow(bf, nl);

		ArrayList<DataPoint<T>> results = new ArrayList<>();
		
		try (Transaction tx = nf.getGraphdb().beginTx()){
			MVPNode<T> topnode = nf.getTopNode();

			Hashtable<Integer,MVPNode<T>> currentnodes = new Hashtable<>(1);
			if (topnode != null) currentnodes.put(0,topnode);

			int n = 0;
			boolean done = false;
			do {
				int nbnodes = (int)Math.pow(bf,n);
				int nbchildnodes = (int)Math.pow(bf,n+nl);
				Hashtable<Integer,MVPNode<T>> childnodes = new Hashtable<>();
				
				for (Enumeration<Integer> e = currentnodes.keys();e.hasMoreElements();){
					int node_index = e.nextElement();
					MVPNode<T> mvpnode = currentnodes.get(node_index);
					int numvps = mvpnode.getNumVantagePoints();
					if (MVPInternal.class.isInstance(mvpnode)){
						MVPInternal<T> internal = (MVPInternal<T>)mvpnode;
						selectChildNodesToQuery(internal, target, childnodes,
												node_index, results, radius);
					} else if (MVPLeaf.class.isInstance(mvpnode)){
						MVPLeaf<T> leaf = (MVPLeaf<T>)mvpnode;
						leaf.filterDataPoints(target, results, radius, metric, nf);
					} else {
						throw new MVPNodeException("unrecognized node type");
					}
				}
				currentnodes = childnodes;
				n += nl;
				if (childnodes.isEmpty())
					done = true;
			} while (!done);

			// results = sortResults(target, results);
			tx.success();
		} catch (Exception ex) {
			throw new MVPTreeException("Unable to query", ex);
		}
		return results;
	}
}
	
