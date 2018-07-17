package org.phash.mvp;

/**
 * Describe shape of tree
 * @author dgs
 * @version 1.0
 */
public class MVPTreeStats {

	/** Total number of points in data base. */
	public int n_total_points;

	/** Total number vantage points **/
	public int n_vps;
	
	/** Total number of non-vp points in graph database. */
	public int n_points;

	/** No. internal nodes in tree */
	public int n_internal;

	/** No. leaf nodes in tree  */
	public int n_leaf;

	/** No. leaf nodes at bottom level */
	public int n_fringe_nodes;

	/** Depth of tree */
	public int depth;

	/** Max. Leaf Size */
	public int max_leaf_size;

	/** Min. Leaf Size */
	public int min_leaf_size;

	/** Avg. leaf size */
	public float avg_leaf_size;  

	/** Constructor */
	public MVPTreeStats(){}
}
