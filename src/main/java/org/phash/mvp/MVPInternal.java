package org.phash.mvp;

import java.util.ArrayList;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.DynamicLabel;


/** MVPInternal node 
 *  @author dgs
 *  @version 0.1
 **/
class MVPInternal<T extends Number> extends MVPNode<T> {

	/** float[] property on internal node **/
	private static final String SplitsProperty = "SPLITS"; 

	/** int property on relationship TO_CHILD **/
	private static final String ChildOrdinalProperty =  "CHILD";  

	private static final String InternalLabel = "INTERNAL";

	/** Constructor
	 * @param Node neo4j node
	 **/
	protected MVPInternal(Node node){
		super(node);
	}

	protected void setNodeType(){
		int node_type = NodeType.INTERNAL_NODE.ordinal();
		getNode().setProperty(NodeTypeProperty, node_type);
	}
	
	protected void setLabel(){
		getNode().addLabel(DynamicLabel.label(InternalLabel));
	}
	
	protected void setSplitsAtLevel(float[] splits, int n){
		StringBuilder str = new StringBuilder(SplitsProperty);
		str.append(n);
		getNode().setProperty(str.toString(), splits);
	}

	protected float[] getSplitsAtLevel(int n){
		StringBuilder str = new StringBuilder(SplitsProperty);
		str.append(n);
		float[] splits = (float[])getNode().getProperty(str.toString(), null);
		return splits;
	}

	protected void setChildNodeAt(MVPNode<?> childNode, int n){
		Relationship rel = getNode().createRelationshipTo(
							  childNode.getNode(), MVPRelationshipTypes.TO_CHILD);
		rel.setProperty(ChildOrdinalProperty, n);
	}

	protected MVPNode<T> getChildNode(int n){
		MVPNode<T> childNode = null;
		Iterable<Relationship> rels = getNode().getRelationships(
								MVPRelationshipTypes.TO_CHILD, Direction.OUTGOING);
		for (Relationship rel : rels){
			int ordinalValue = (int)rel.getProperty(ChildOrdinalProperty);
			if (ordinalValue == n){
				Node endNode = rel.getEndNode();
				int nodetype = (int)endNode.getProperty(NodeTypeProperty);
				if (nodetype == NodeType.INTERNAL_NODE.ordinal())
					childNode = new MVPInternal<>(endNode);
				else 
					childNode = new MVPLeaf<>(endNode);
				break;
			}
		}
		return childNode;
	}

	protected void deleteAsChildNode(int n){
		Iterable<Relationship> rels = getNode().getRelationships(
								 MVPRelationshipTypes.TO_CHILD, Direction.OUTGOING);
		for (Relationship rel : rels){
			int ordinalValue = (int)rel.getProperty(ChildOrdinalProperty);
			if (ordinalValue == n){
				rel.delete();
				break;
			}
		}
	}
}

