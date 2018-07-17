# MVP Tree Graph Database

The MVP Tree is a distance-based spatial indexing structure that is particularly well suited
for higher N-dimensional metric spaces.  Instead of indexing data by spatial coordinates, the
mvp tree divides the points according to distances from arbitrarily chosen vantage points.
A list of precomputed distances is stored with each datapoint to indicate that datapoint's
distance from a list of respective vantage points.  This path of distances serves as a filtering
step to reduce the number of distance computations necessary for retrieval of nearest neighbor
queries. 

## Features

* Customizable parameter that define the tree shape - such as branch factor, number levels
  per node, number pre-computed distances, and a minimum capacity for leaf nodes.  

* A Generic type implementation, so data can be an array of any primitive data type.
  (e.g. float[], int[], byte[], long[], short[])

* Non-recursive implementation

* Use of L1, L2 or Hamming metric space distances.

* Ability to customize additional metric spaces.

* Persistent storage of tree and data points to a Neo4j graph database.

* Query for all data points within a given radius of a target data point.
  Nearest-neighbor queries. 

* All data points are indexed for direct retrieval by a string Id.

* Ability to delete points.  

## Parameters

   branch factor (bf) - number of branches off of each internal node (e.g. 2 or 3)
   no. levels    (nl) - number of levels for each internal (non-leaf) node. (e.g. 2, 4 or 8)
   path length   (pl) - number of pre-computed distances to store for each data point. Each
                        distance represents that point's distance from respective vantage point
				        in a path from the root node down to the leaf node in which the data
						point is in. (e.g. 4, 8, 16, ...)
   leaf minimum  (lm) - Minimum data points in a leaf node.  

   Note: Before a leaf node is converted to an internal (non-leaf) node, it must be assigned
   at least (bf^nl)xlm number of datapoints. This ensures that internal nodes are well balanced
   with child nodes containing roughly similar number of datapoints. So, leaf nodes contain
   anywhere betwen lm and ((bf^nl)xlm - 1) data points.


### Metric Space

The tree works for any metric space such that for any two points, x and y,
the following conditions apply:

1. distance(x,y) = distance(y,x)                  (commutative)
2. Inf > distance(x,y) > 0                        (positively bounded)
3. distance(x,y) <= distance(x,z) + distance(z,y) (triangle inequality)


### Instructions
```
mvn package
mvn test
mvn install
mvn javadoc
```

### Dependencies

   * Neo4j v3.0.1
   * Appache Commons Lang v3.7
   * JCommander v1.30
   * JUnit Testing framework v4.12


### References

   [Bozkaya, Ozsoyoglu, 1999](https://dl.acm.org/citation.cfm?id=253345)

