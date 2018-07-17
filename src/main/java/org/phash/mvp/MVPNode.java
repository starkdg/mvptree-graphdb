package org.phash.mvp;

import java.util.ArrayList;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Label;


/* Node type (for internal use)*/
enum NodeType {	INTERNAL_NODE, LEAF_NODE;}


/** Abstract MVPNode 
 * Base functionality common to both MVPInternal
 * and MVPLeaf Nodes
 * @author dgs
 * @version 0.1
 **/

abstract class MVPNode<T extends Number> {
	/* neo4j node */
	private final Node node;
	
	/* relevant neo4j properties */
	protected static final String NodeTypeProperty = "NODETYPE";
	protected static final String VPOrdinalProperty = "VP";

	/** Constructor 
	 * @param Node neo4j node
	 **/
	protected MVPNode(Node node){
		this.node = node;
	}

	protected Node getNode(){ return node;}

	/* Mark as the top node in the tree */
	protected void setAsTop(NodeFactory nf){
		getNode().addLabel(Label.label(NodeFactory.TopLabel));
		getNode().setProperty(NodeFactory.TopProperty, NodeFactory.TopN);
		nf.saveParameters();
	}

	/* Is the node the top node in the tree? */
	protected boolean isTopNode(){
		boolean is_top  = getNode().hasLabel(Label.label(NodeFactory.TopLabel));
		return is_top;
	}

	protected void removeAsTop(){
		getNode().removeLabel(Label.label(NodeFactory.TopLabel));
		getNode().removeProperty(NodeFactory.TopProperty);
	}
	
	boolean isSameAs(MVPNode mvpnode){
		if  (mvpnode != null && getNode().getId() == mvpnode.getNode().getId())
			return true;
		return false;
	}

	protected void setVantagePoint(DataPoint<?> point, int n){
		Relationship rel = getNode().createRelationshipTo(
				 point.getNode(), MVPRelationshipTypes.TO_VP);
		rel.setProperty(VPOrdinalProperty, n);
	}

	protected int getNumVantagePoints(){
		int count = 0;
		Iterable<Relationship> rels = getNode().getRelationships(
					  MVPRelationshipTypes.TO_VP, Direction.OUTGOING);
		for (Relationship rel : rels) count++;
		return count;
	}
	
	protected void selectVantagePoints(ArrayList<DataPoint<T>> points, int count, int numvps){
		int i = count;
		while (i <  numvps && points.size() > 0) {
			DataPoint<?> vp = points.remove(0);
			Relationship rel = getNode().createRelationshipTo(
					vp.getNode(), MVPRelationshipTypes.TO_VP);
			rel.setProperty(VPOrdinalProperty, i++);
		};
	}

	protected DataPoint<T> getVantagePoint(int n, NodeFactory<T> nf){
		DataPoint<T> dp = null;
		Iterable<Relationship> rels = getNode().getRelationships(
					MVPRelationshipTypes.TO_VP, Direction.OUTGOING);
		for (Relationship rel : rels){
			int ordinalValue = (int)rel.getProperty(VPOrdinalProperty);
			if (ordinalValue == n){
				dp = nf.wrapDataPoint(rel.getEndNode());
				break;
			}
		}
		if (dp == null)
			throw new MVPNodeException("no vantage point at " + n);
		return dp;
	}

	protected ArrayList<DataPoint<T>> getVantagePoints(int numvps, NodeFactory<T> nf){
		ArrayList<DataPoint<T>> vps = new ArrayList<DataPoint<T>>();
		for (int i=0;i<numvps;i++){
			DataPoint<T> vp = getVantagePoint(i, nf);
			vps.add(vp);
		}
		return vps;
	}

	protected void delete(){
		removeAsTop();
		Iterable<Relationship> rels = getNode().getRelationships();
		for (Relationship rel : rels){
			rel.delete();
		}
		getNode().delete();
	}
}
