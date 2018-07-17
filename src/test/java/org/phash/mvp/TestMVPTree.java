package org.phash.mvp;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.FixMethodOrder;

import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Random;
import java.net.URL;
import java.io.File;
import java.io.PrintStream;
/**
 * 
 */
@RunWith(Parameterized.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMVPTree {

	@Parameters(name = "Test:{index}:mvp(bf={0},pl={1},lm={2},nl={3})")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{3, 6, 10, 3}
			});
	}
	
	@Parameter (value = 0)
	public int bf;
	
	@Parameter (value = 1)
	public int pl;

	@Parameter (value = 2)
	public int lm;

	@Parameter (value = 3)
	public int nl;

	public final int ncenters = 10;
	public final int ndims = 20;
	
	public String dbstore = "var/graphdb";
	public String propsfile = "/conf/mvptree-neo4j.properties";

	public static Float[][] centers;

	public static int point_id = 1;
	public static int center_id = 1;
	public static Random rnd;
	public static MVPTree<Float> tree;

	public TestMVPTree(){}
	
	private void fill(Float[] data){
		for (int i=0;i<data.length;i++){
			data[i] = rnd.nextFloat();
		}
	}
	
	private DataPoint<Float> generateRandomDataPoint(MVPTree<Float> tree){
		DataPoint<Float> pnt = null;
		try {
			Float[] data = new Float[ndims];
			fill(data);
			pnt = tree.createDataPoint();
			pnt.setId("Point" + point_id++);
			pnt.setData(data);
		} catch (Exception ex){
			System.out.printf("Error: %s\n", ex.getMessage());
			ex.printStackTrace();
		}
		return pnt;
	}
	
	private ArrayList<DataPoint<Float>> generateUniformRandomDataPoints(int n_points,
																	   MVPTree mvptree){
		ArrayList<DataPoint<Float>> points = mvptree.createDataPoints(n_points);
		for (DataPoint<Float> pnt : points){
			Float[] data = new Float[ndims];
			fill(data);
			pnt.setId("Point" + point_id++);
			pnt.setData(data);
		}
		return points;
	}

	private ArrayList<DataPoint<Float>> generateCluster(int n_points,
														float epsilon,
														MVPTree tree){
		ArrayList<DataPoint<Float>> points = new ArrayList<DataPoint<Float>>();
		DataPoint<Float> centerPoint = tree.createDataPoint();
		Float[] centerData = new Float[ndims];
		fill(centerData);
		centerPoint.setId("ClusterCenter" + center_id++);
		centerPoint.setData(centerData);
		points.add(0, centerPoint);
		for (int i=1;i<n_points;i++){
			DataPoint<Float> pnt = tree.createDataPoint();
			Float[] data = new Float[ndims];
			for (int j=0;j<ndims;j++){
				float tweak = rnd.nextFloat()*2*epsilon - epsilon;
				data[j] = centerData[j] + tweak;
			}
			pnt.setId("ClusterPoint" + point_id++);
			pnt.setData(data);
			points.add(pnt);
		}
		return points;
	}

	@BeforeClass static public void init(){
		System.out.printf("init Random Number generator\n");
		rnd = new Random(19839812982L);
		for (int i=0;i<1000;i++)rnd.nextFloat();

		int n_centers = 10;
		centers = new Float[n_centers][];
	}

	@Test public void test0(){
		System.out.printf("----------Test--with Float[] data ------\n");
		System.out.printf("-------(bf=%d, pl=%d, lm=%d, nl=%d)-----\n", bf, pl, lm, nl);

		String conffile = getClass().getResource(propsfile).getFile();
		tree = new MVPTree<>(dbstore, conffile, bf, pl, lm, nl,
							 DistanceFunction.L1,
							 Float.class);
		Assert.assertNotNull(tree);
	}

	@Test public void test1(){
		int count = 0;
		int n = 1000;
		int iters = 10;
		int original_count = tree.getDataPointCount();

		System.out.printf("Test - Add %d Uniformly random points.\n", n);
		try {
			for (int i=0;i<iters;i++){
				System.out.printf("  Add %d points.\n", n);
				ArrayList<DataPoint<Float>> points = generateUniformRandomDataPoints(n, tree);
				tree.addPoints(points);
				count += n;
				Assert.assertTrue(tree.getDataPointCount() == original_count + count);
			}
		} catch (Exception ex){
			System.out.println("test 1 failed: " + ex.getMessage());
			ex.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Ignore public void test2(){
		System.out.println("Print tree.");
		try {
			tree.printTree(System.out);
		} catch (Exception ex){
			System.out.println("unable to print tree: " + ex.getMessage());
			Assert.assertTrue(false);
		}
	}
	
	@Test public void test3(){
		int n = 10;
		float epsilon = 0.10f;
		System.out.printf("Add %d clusters of %d points with radius %f\n",
						  ncenters, n, epsilon);

		try {
			int original_count = tree.getDataPointCount();
			for (int i=0;i<ncenters;i++){
				ArrayList<DataPoint<Float>> cluster = generateCluster(n, epsilon, tree);
				centers[i] = cluster.get(0).getData();
				System.out.printf("  Add cluster %s of %d points\n", cluster.get(0).getId(), n);
				tree.addPoints(cluster);
				Assert.assertTrue(tree.getDataPointCount() == original_count + n*(i+1));
			}
		} catch (Exception ex){
			System.out.println("test 2 failed: " + ex.getMessage());
			ex.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Test public void test4(){
		System.out.printf("Test Query - %d queries\n", ncenters);
		int total = tree.getDataPointCount();
		try {
			float radius = 0.10f;
			int sum_ops = 0;
			for (int i=0;i<ncenters;i++){
				TargetPoint<Float> target = new TargetPoint<>(centers[i]);

				DataPoint.reset();
				Collection<DataPoint<Float>> results = tree.queryTarget(target, radius);
				sum_ops += DataPoint.getNumOpsCount();

				double pct_ops = (double)sum_ops/(double)total;
				System.out.printf("  Found %d points\n", results.size());
				Assert.assertTrue(results.size() >= 10);
			}
			double pct_ops = (double)sum_ops/(double)ncenters/(double)total;
			System.out.printf(" %.4f distance calculations\n", pct_ops);
		} catch (Exception ex){
			System.out.println("test 3 failed: " + ex.getMessage());
			ex.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Test public void test5(){
		try {
			MVPTreeStats stats = new MVPTreeStats();
			tree.stats(stats);
			System.out.println("Tree Stats.");
			System.out.println("no. points: " + stats.n_total_points);
			System.out.println("no. vps points: " + stats.n_vps);
			System.out.println("no. non-vp points: " + stats.n_points);
			System.out.println("no. internal nodes: " + stats.n_internal);
			System.out.println("no. leaf nodes: " + stats.n_leaf);
			System.out.println("no. fringe nodes: " + stats.n_fringe_nodes);
			System.out.println("tree depth: " + stats.depth);
			System.out.println("max. leaf size: " + stats.max_leaf_size);
			System.out.println("min. leaf size: " + stats.min_leaf_size);
			System.out.println("avg. leaf size: " + stats.avg_leaf_size);
			
		} catch (Exception ex){
			System.out.println("test 4 failed: " + ex.getMessage());
			ex.printStackTrace();
			Assert.assertTrue(false);
		}
		
	}

	@Test public void test6(){
		System.out.printf("Test Clear.\n");
		tree.clear();
		Assert.assertTrue(tree.getDataPointCount() == 0);
	}

}
