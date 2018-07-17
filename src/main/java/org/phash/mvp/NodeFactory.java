package org.phash.mvp;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.MultipleFoundException;

import java.io.File;
import java.util.ArrayList;

/**
 * <h1>NodeFactory</h1>
 * Provide methods to access mvptree and DataObjects
 * in graph database.
 * @author dgs
 * @version 0.1
 */
public class NodeFactory<T extends Number> {

	/* properties and labels in neo4j graph database */
	protected static final String TopLabel = "TOP";
	protected static final String TopProperty = "TOPN";
	protected static final int TopN = 0;
	protected static final String BranchFactorProperty = "BRANCHFACTOR";
	protected static final String PathLengthProperty = "PATHLENGTH";
	protected static final String LeafCapacityProperty = "LEAFCAPACITY";
	protected static final String DataPointIndexProperty = "DATAPOINTS";
	protected static final String PointNameProperty = "POINTNAME";
	protected static final String LevelsPerNodeProperty = "NLEVELSPERNODE";

	private String GraphdbDir;
	private String PropertiesFile;
	private int BranchFactor, PathLength, LeafMinimum, NumLevelsPerNode;
	private final Class<T> type;
	private GraphDatabaseService graphdb;

	private void registerShutdownHook(final GraphDatabaseService graphdb){
		Runtime.getRuntime().addShutdownHook(new Thread(){
				@Override public void run(){
					graphdb.shutdown();
				}
			});
	}

	/** Constructor
	 * 
	 * @param String    graphdb directory
	 * @param String    neo4j properties file (e.g. "conf/mvptree-neo4j.properties")
	 * @param int       branchfactor (generally 2 or 3)
	 * @param int       pathlength (e.g. 8)
	 * @param int       leaf minimum (e.g. 30) no. DataPoints to a leaf.
	 * @param int       number of levels per (internal) mvp node
     * @param Class<T>  class of generic type (necessary to determine at runtime)
	 */
	protected NodeFactory(String graphdbdir, String propsFile,
						  int bf, int pl, int lm, int nl, Class<T> type){
		if (bf <= 0) throw new IllegalArgumentException("bf <= 0");
		if (pl <= 0) throw new IllegalArgumentException("pl <= 0");
		if (lm <= 0) throw new IllegalArgumentException("lc <= 0");
		if (nl <= 0) throw new IllegalArgumentException("nl <= 0");

		this.BranchFactor = bf;
		this.PathLength   = pl;
		this.LeafMinimum  = lm;
		this.NumLevelsPerNode = nl;
		this.GraphdbDir = graphdbdir;
		this.PropertiesFile = propsFile;
		this.graphdb = null;
		this.type = type;
		initGraphDatabase();
		try (Transaction tx = graphdb.beginTx()){
			getParameters();
			tx.success();
		}
		registerShutdownHook(graphdb);
	}

	/** get fields **/
	protected int getBranchFactor(){return BranchFactor;}
	protected int getPathLength(){return PathLength;}
	protected int getLeafMinimum(){return LeafMinimum;}
	protected int getNumLevelsPerNode(){return NumLevelsPerNode;}
	protected String getGraphDBFile(){ return GraphdbDir;}
	protected String getPropsFile(){ return PropertiesFile;}
	protected GraphDatabaseService getGraphdb(){return graphdb;}
	
	protected void initGraphDatabase(){
		if (graphdb != null)
			return;
		
		File storeDir = new File(GraphdbDir);
		if (!storeDir.exists()) storeDir.mkdirs();
		
		GraphDatabaseBuilder builder = new GraphDatabaseFactory(
                                       ).newEmbeddedDatabaseBuilder(storeDir);
		if (PropertiesFile != null && !PropertiesFile.isEmpty())
			builder.loadPropertiesFromFile(PropertiesFile);
		graphdb = builder.newGraphDatabase();
	}

	protected void shutdown(){
		graphdb.shutdown();
		graphdb = null;
	}


	/* Retrieve parameters from neo4j database, if there is a top reference node */
	protected void getParameters(){
		Node top = getRefNode();
		if (top != null){
			BranchFactor = (int)top.getProperty(BranchFactorProperty);
			PathLength = (int)top.getProperty(PathLengthProperty);
			LeafMinimum = (int)top.getProperty(LeafCapacityProperty);
			NumLevelsPerNode = (int)top.getProperty(LevelsPerNodeProperty);
		}
	}

	/* Save parameters as properties in reference node */ 
	protected void saveParameters(){
		Node top = getRefNode();
		if (top != null){
			top.setProperty(BranchFactorProperty, BranchFactor);
			top.setProperty(PathLengthProperty, PathLength);
			top.setProperty(LeafCapacityProperty, LeafMinimum);
			top.setProperty(LevelsPerNodeProperty, NumLevelsPerNode);
		}
	}

	/* Get the Top node that exists below the reference node */
	protected Node getRefNode(){
		Node node = null;
		int count = 0;
		while (count < 5){
			try {
				node = graphdb.findNode(Label.label(NodeFactory.TopLabel),
										TopProperty, TopN);
				count += 5;
			}catch (MultipleFoundException ex){
				if (count >= 5)
					throw new MVPTreeException("found more than one top node", ex);
				try {
					Thread.sleep(1000, 0);
				} catch (InterruptedException ex2){
					System.err.printf("interrupted: %s\n", ex2.getMessage());
				}
			}
		}
		return node;
	}
	
	/* get the top MVPNode of MVPTree */
	protected MVPNode<T> getTopNode(){
		MVPNode<T> resultnode = null;
		Node refNode = getRefNode();
		if (refNode != null){
			resultnode  = wrapNode(refNode);
		}
		return resultnode;
	}

	/* utility function to wrap a node in MVPInternal or MVPLeaf node */
	protected MVPNode<T> wrapNode(Node node){
		MVPNode<T> resultNode = null;
		int node_type = (int)node.getProperty(MVPNode.NodeTypeProperty);
		if (node_type == NodeType.INTERNAL_NODE.ordinal()){
			resultNode =  (MVPNode<T>)new MVPInternal<>(node);
		} else if (node_type == NodeType.LEAF_NODE.ordinal()){
			resultNode = (MVPNode<T>)new MVPLeaf<>(node);
		}
		return resultNode;
	}

	/* Create MVPInternal node for a list of DataPoints */
	protected MVPInternal<T> createInternalNode(ArrayList<DataPoint<T>> points){
		Node new_node = graphdb.createNode();
		MVPInternal<T> internal = new MVPInternal<>(new_node);
		internal.setNodeType();
		internal.setLabel();
		internal.selectVantagePoints(points, 0, getNumLevelsPerNode());
		return internal;
	}

	/* Create MVPLeaf node for a list of DataPoints */
	protected MVPLeaf<T> createLeafNode(ArrayList<DataPoint<T>> points){
		Node new_node = graphdb.createNode();
		MVPLeaf<T> leaf = new MVPLeaf<>(new_node);
		leaf.setNodeType();
		leaf.setLabel();
		leaf.selectVantagePoints(points, 0, getPathLength());
		return leaf;
	}

	/* Index DataPoints by String id */
	protected void indexPoints(Iterable<DataPoint<T>> points){
		IndexManager index = getGraphdb().index();
		Index<Node> nodeIndex = index.forNodes(DataPointIndexProperty);
		for (DataPoint<?> point : points){
			nodeIndex.add(point.getNode(), PointNameProperty, point.getId());
			point.setActive(true);
		}
	}

	/* Empty the index */
	protected void clearDataPointIndex(){
		IndexManager index = graphdb.index();
		Index<Node> nodeIndex = index.forNodes(DataPointIndexProperty);
		nodeIndex.delete();
	}

	/* Get total number of DataPoints indexed in tree */
	protected int getCount(){
		int count = 0;
		Index<Node> nodeIndex = graphdb.index().forNodes(DataPointIndexProperty);
		if (nodeIndex != null) {
			IndexHits<Node>  points = nodeIndex.query(PointNameProperty, "*");
			count = points.size();
		}
		return count;
	}

	/* Look up DataPoint by String id */
	protected DataPoint<T> lookupDataPoint(String id){
		DataPoint<T> pnt = null;
		IndexManager index = graphdb.index();
		Index<Node> nodeIndex = index.forNodes(DataPointIndexProperty);
		IndexHits<Node> hits = nodeIndex.get(PointNameProperty, id);
		Node dpnode = hits.getSingle();
		if (dpnode != null)
			pnt = new DataPoint<>(dpnode, getPathLength(), type);
		return pnt;
	}

	/* Delete a DataPoint with String id */
	protected void deleteDataPoint(String id){
		IndexManager index = graphdb.index();
		Index<Node> nodeIndex = index.forNodes(DataPointIndexProperty);
		IndexHits<Node> hits = nodeIndex.get(PointNameProperty, id);
		Node node = hits.getSingle();
		if (node != null) {
			DataPoint<?> point = new DataPoint<>(node, getPathLength(), type);
			point.setActive(false);
		}
		nodeIndex.remove(node, PointNameProperty, id);
	}

	/* Create a new DataPoint object in neo4j database */
	/* setId() and setData() must still be called on DataPoint */
	protected DataPoint<T> createDataPoint(){
		Node new_node = getGraphdb().createNode();
		DataPoint<T> pnt = new DataPoint(new_node, getPathLength(), type);
		pnt.setActive(true);
		pnt.setLabel();
		return pnt;
	}

	/* Wrap a neo4j node object in a DataPoint */
	protected DataPoint<T> wrapDataPoint(Node node){
		DataPoint<T> dp = new DataPoint<>(node, getPathLength(), type);
		return dp;
	}

	/* Wrap a neo4j node object in a DataPoint with a path of float[] */
	protected DataPoint<T> wrapDataPoint(Node node, float[] path){
		
		DataPoint<T> dp = new DataPoint<>(node, path, type);
		return dp;
	}

	/* Create a batch of n empty DataPoints */
	/* Must still call setId() and setData() methods on each DataPoint */
	protected ArrayList<DataPoint<T>> createDataPoints(int n){
		ArrayList<DataPoint<T>> points = new ArrayList<DataPoint<T>>(n);
		for (int i=0;i<n;i++){
			DataPoint<T> pnt = createDataPoint();
			points.add(pnt);
		}
		return points;
	}
}
