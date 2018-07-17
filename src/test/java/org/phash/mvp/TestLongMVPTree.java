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
public class TestLongMVPTree {

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
	public final int ndims = 10;
	
	public String dbstore = "var/graphdb2";
	public String propsfile = "/conf/mvptree-neo4j.properties";

	public static Long[][] centers;

	public static int point_id = 1;
	public static int center_id = 1;
	public static Random rnd;
	public static MVPTree<Long> tree;

	public TestLongMVPTree(){}
	
	private void fill(Long[] data){
		for (int i=0;i<data.length;i++){
			data[i] = rnd.nextLong();
		}
	}
	
	private DataPoint<Long> generateRandomDataPoint(MVPTree<Long> tree){
		DataPoint<Long> pnt = null;
		try {
			Long[] data = new Long[ndims];
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
	
	private ArrayList<DataPoint<Long>> generateUniformRandomDataPoints(int n_points,
																	   MVPTree mvptree){
		ArrayList<DataPoint<Long>> points = mvptree.createDataPoints(n_points);
		for (DataPoint<Long> pnt : points){
			Long[] data = new Long[ndims];
			fill(data);
			pnt.setId("Point" + point_id++);
			pnt.setData(data);
		}
		return points;
	}

	private void flip_bit(Long[] data, int bit_index){
		int index = bit_index / Long.SIZE;
		int shiftby = bit_index % Long.SIZE;
		long one = 0x0001L;
		long mask = one << shiftby;
		data[index] = data[index].longValue() ^ mask;
	}

	private ArrayList<DataPoint<Long>> generateCluster(int n_points,
														int epsilon,
														MVPTree tree){
		ArrayList<DataPoint<Long>> points = new ArrayList<DataPoint<Long>>();
		DataPoint<Long> centerPoint = tree.createDataPoint();
		Long[] centerData = new Long[ndims];
		fill(centerData);
		centerPoint.setId("ClusterCenter" + center_id++);
		centerPoint.setData(centerData);
		points.add(0, centerPoint);
		for (int i=1;i<n_points;i++){
			DataPoint<Long> pnt = tree.createDataPoint();
			Long[] data = centerData.clone();
			for (int j=0;j<epsilon;j++){
				int bit_index = rnd.nextInt(Long.SIZE*ndims);
				flip_bit(data, bit_index);
			}
			pnt.setId("ClusterPoint" + point_id++);
			pnt.setData(data);
			points.add(pnt);
		}
		return points;
	}

	@BeforeClass static public void init(){
		System.out.printf("init Random Number generator\n");
		rnd = new Random(208342930L);
		for (int i=0;i<1000;i++)rnd.nextLong();

		int n_centers = 10;
		centers = new Long[n_centers][];
	}

	@Test public void test0(){
		System.out.printf("-------Test with Long[] data------------\n");
		System.out.printf("------(bf=%d, pl=%d, lm=%d, nl=%d)------\n", bf, pl, lm, nl);;

		String conffile = getClass().getResource(propsfile).getFile();
		this.tree = new MVPTree<>(dbstore, conffile, bf, pl, lm, nl,
								  DistanceFunction.HAMMING, Long.class);
		Assert.assertNotNull(tree);
	}

	@Test public void test1(){
		int count = 0;
		int n = 1000;
		int iters = 10;
		System.out.printf("Add %d uniformly random points\n", n);

		try {
			int original_count = tree.getDataPointCount();
			for (int i=0;i<iters;i++){
				System.out.printf("  Add %d points.\n", n);
				ArrayList<DataPoint<Long>> points = generateUniformRandomDataPoints(n, tree);
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

	
	@Test public void test2(){
		int n = 10;
		int epsilon = 5;
		int count = 0;

		System.out.printf("Add %d clusters of radius %d\n", ncenters, epsilon);
		try {
			int original_count = tree.getDataPointCount();
			for (int i=0;i<ncenters;i++){
				ArrayList<DataPoint<Long>> cluster = generateCluster(n, epsilon, tree);
				centers[i] = cluster.get(0).getData();
				System.out.printf("  Add cluster %s of %d points.\n", cluster.get(0).getId(), n);
				tree.addPoints(cluster);
				count += n;
				Assert.assertTrue(tree.getDataPointCount() == original_count + count);
			}
		} catch (Exception ex){
			System.out.println("test 2 failed: " + ex.getMessage());
			ex.printStackTrace();
			Assert.assertTrue(false);
		}
	}

	@Ignore public void test3(){
		System.out.println("Print tree.");
		try {
			tree.printTree(System.out);
		} catch (Exception ex){
			System.out.println("unable to print tree: " + ex.getMessage());
			Assert.assertTrue(false);
		}
	}

	@Test public void test4(){
		System.out.println("Test Query");
		int total = tree.getDataPointCount();
		
		try {
			float radius = 5.0f;
			int sum_ops = 0;
			for (int i=0;i<ncenters;i++){
				TargetPoint<Long> target = new TargetPoint<>(centers[i]);

				DataPoint.reset();
				Collection<DataPoint<Long>> results = tree.queryTarget(target, radius);
				sum_ops += DataPoint.getNumOpsCount();

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
		System.out.printf("Test Tree Statistics.\n");
		try {
			MVPTreeStats stats = new MVPTreeStats();
			tree.stats(stats);
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
		System.out.printf("Test Clear tree\n");
		tree.clear();
		Assert.assertTrue(tree.getDataPointCount() == 0);
	}

}
