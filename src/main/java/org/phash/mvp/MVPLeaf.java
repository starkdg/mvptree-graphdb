package org.phash.mvp;

import java.util.Collection;
import java.util.Vector;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.DynamicLabel;

import java.util.ArrayList;

class MVPLeaf<T extends Number> extends MVPNode<T> {

	private static final String PathProperty = "PATH";
	private static final String LeafLabel = "LEAF";
	
	protected MVPLeaf(Node node){
		super(node);
	}

	protected void setNodeType(){
		int node_type = NodeType.LEAF_NODE.ordinal();
		getNode().setProperty(NodeTypeProperty, node_type);
	}
	
	protected void setLabel(){
		getNode().addLabel(DynamicLabel.label(LeafLabel));
	}
	
	protected int getDataPointCount(){
		int count = 0;
		Iterable<Relationship> rels = getNode().getRelationships(
						  MVPRelationshipTypes.TO_DP, Direction.OUTGOING);
		for (Relationship rel : rels) count++;
		return count;
	}

	protected void addDataPoint(DataPoint<?> point){
		Relationship rel = getNode().createRelationshipTo(
							point.getNode(), MVPRelationshipTypes.TO_DP);
		rel.setProperty(PathProperty, point.getPath());
	}


	protected void addDataPoints(ArrayList<DataPoint<T>> points){
		for (DataPoint pnt : points){
			Relationship rel = getNode().createRelationshipTo(
							  pnt.getNode(), MVPRelationshipTypes.TO_DP);
			rel.setProperty(PathProperty, pnt.getPath());
		}
		points.clear();
	}

	protected int filterDataPoints(TargetPoint<T> target,
								   ArrayList<DataPoint<T>> results,
								   float radius,
								   MetricDistance metric,
								   NodeFactory<T> nf){
		int count = 0;
		int numvps = getNumVantagePoints();
		ArrayList<Float> qdists = new ArrayList<Float>();
		for (int i=0;i<numvps;i++){
			DataPoint<T> vp = getVantagePoint(i, nf);
			Double d = metric.distance(vp, target);
			if (vp.isActive() && d.floatValue() <= radius)
				results.add(vp);
			qdists.add(d.floatValue());
		}

		Iterable<Relationship> rels = getNode().getRelationships(
									 MVPRelationshipTypes.TO_DP, Direction.OUTGOING);
		for (Relationship rel : rels){
			float[] pdists = (float[])rel.getProperty(PathProperty);
			boolean skip = false;
			for (int i=0;i < numvps;i++){
				if (!(pdists[i] >= qdists.get(i) - radius && pdists[i] <= qdists.get(i) + radius)){
					skip=true;
					break;
				}
			}
			if (!skip){
				DataPoint<T> pnt = nf.wrapDataPoint(rel.getEndNode(), pdists);
				Double d = metric.distance(pnt, target);
				if (!pnt.isActive()) pnt.delete();
				else if (d.floatValue() <= radius){
					results.add(pnt);
					count++;
				}
			}
		}
		return count;
	}
	
	protected ArrayList<DataPoint<T>> getAllDataPoints(NodeFactory<T> nf){
		ArrayList<DataPoint<T>> points = new ArrayList<>();
		Iterable<Relationship> rels = getNode().getRelationships(
						   MVPRelationshipTypes.TO_DP, Direction.OUTGOING);
		for (Relationship rel : rels){
			float[] path = (float[])rel.getProperty(PathProperty);
			DataPoint<T> point = nf.wrapDataPoint(rel.getEndNode(), path);
			if (point.isActive()){
				points.add(point);
			} else {
				point.delete();
			}
		}
		return points;
	}
}
