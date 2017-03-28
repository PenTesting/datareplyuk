"""
Program:    RUNNING GRAPH ANALYTICS WITH SPARK GRAPH-FRAMES:
Author:     Dr. C. Hadjinikolis
Date:       14/09/2016
Description:    This is the application's core module from where everything is executed.
                The module is responsible for:
                1. Loading Spark
                2. Loading GraphFrames
                3. Running analytics by leveraging other modules in the package.
"""
# IMPORT OTHER LIBS -------------------------------------------------------------------------------#
import os
import sys
import pandas as pd

# IMPORT SPARK ------------------------------------------------------------------------------------#
# Path to Spark source folder
USER_FILE_PATH = "/Users/christoshadjinikolis"
SPARK_PATH = "/PycharmProjects/GenesAssociation"
SPARK_FILE = "/spark-2.0.0-bin-hadoop2.7"
SPARK_HOME = USER_FILE_PATH + SPARK_PATH + SPARK_FILE
os.environ['SPARK_HOME'] = SPARK_HOME

# Append pySpark to Python Path
sys.path.append(SPARK_HOME + "/python")
sys.path.append(SPARK_HOME + "/python" + "/lib/py4j-0.10.1-src.zip")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SQLContext
    from pyspark.graphframes import graphframe as GF

except ImportError as ex:
    print "Can not import Spark Modules", ex
    sys.exit(1)

# GLOBAL VARIABLES --------------------------------------------------------------------------------#
# Configure spark properties
CONF = (SparkConf()
        .setMaster("local")
        .setAppName("My app")
        .set("spark.executor.memory", "10g")
        .set("spark.executor.instances", "4"))

# Instantiate SparkContext object
SC = SparkContext(conf=CONF)

# Instantiate SQL_SparkContext object
SQL_CONTEXT = SQLContext(SC)

# MAIN CODE ---------------------------------------------------------------------------------------#
if __name__ == "__main__":

    # Main Path to CSV files
    DATA_PATH = '/PycharmProjects/GenesAssociation/data/'
    FILE_NAME = 'gene_gene_associations_50k.csv'

    # LOAD DATA CSV USING  PANDAS -----------------------------------------------------------------#
    print "STEP 1: Loading Gene Nodes -------------------------------------------------------------"
    # Read csv file and load as df
    GENES = pd.read_csv(USER_FILE_PATH + DATA_PATH + FILE_NAME,
                        usecols=['OFFICIAL_SYMBOL_A'],
                        low_memory=True,
                        iterator=True,
                        chunksize=1000)

    # Concatenate chunks into list & convert to dataFrame
    GENES_DF = pd.DataFrame(pd.concat(list(GENES), ignore_index=True))

    # Remove duplicates
    GENES_DF_CLEAN = GENES_DF.drop_duplicates(keep='first')

    # Name Columns
    GENES_DF_CLEAN.columns = ['id']

    # Output dataFrame
    print GENES_DF_CLEAN

    # Create vertices
    VERTICES = SQL_CONTEXT.createDataFrame(GENES_DF_CLEAN)

    # Show some vertices
    print VERTICES.take(5)

    print "STEP 2: Loading Gene Edges -------------------------------------------------------------"
    # Read csv file and load as df
    EDGES = pd.read_csv(USER_FILE_PATH + DATA_PATH + FILE_NAME,
                        usecols=['OFFICIAL_SYMBOL_A', 'OFFICIAL_SYMBOL_B', 'EXPERIMENTAL_SYSTEM'],
                        low_memory=True,
                        iterator=True,
                        chunksize=1000)

    # Concatenate chunks into list & convert to dataFrame
    EDGES_DF = pd.DataFrame(pd.concat(list(EDGES), ignore_index=True))

    # Name Columns
    EDGES_DF.columns = ["src", "dst", "rel_type"]

    # Output dataFrame
    print EDGES_DF

    # Create vertices
    EDGES = SQL_CONTEXT.createDataFrame(EDGES_DF)

    # Show some edges
    print EDGES.take(5)

    print "STEP 3: Generating the Graph -----------------------------------------------------------"

    GENES_GRAPH = GF.GraphFrame(VERTICES, EDGES)

    print "STEP 4: Running Various Basic Analytics ------------------------------------------------"
    print "Vertex in-Degree -----------------------------------------------------------------------"
    GENES_GRAPH.inDegrees.sort('inDegree', ascending=False).show()
    print "Vertex out-Degree ----------------------------------------------------------------------"
    GENES_GRAPH.outDegrees.sort('outDegree', ascending=False).show()
    print "Vertex degree --------------------------------------------------------------------------"
    GENES_GRAPH.degrees.sort('degree', ascending=False).show()
    print "Triangle Count -------------------------------------------------------------------------"
    RESULTS = GENES_GRAPH.triangleCount()
    RESULTS.select("id", "count").show()
    print "Label Propagation ----------------------------------------------------------------------"
    GENES_GRAPH.labelPropagation(maxIter=10).show()     # Convergence is not guaranteed
    print "PageRank -------------------------------------------------------------------------------"
    GENES_GRAPH.pageRank(resetProbability=0.15, tol=0.01)\
        .vertices.sort('pagerank', ascending=False).show()

    print "STEP 5: Find Shortest Paths w.r.t. Landmarks -------------------------------------------"
    # Shortest paths
    SHORTEST_PATH = GENES_GRAPH.shortestPaths(landmarks=["ARF3", "MAP2K4"])
    SHORTEST_PATH.select("id", "distances").show()

    print "STEP 6: Save Vertices and Edges --------------------------------------------------------"
    # Save vertices and edges as Parquet to some location.
    # Note: You can't overwrite existing vertices and edges directories.
    GENES_GRAPH.vertices.write.parquet("vertices")
    GENES_GRAPH.edges.write.parquet("edges")

    print "STEP 7: Load "
    # Load the vertices and edges back.
    SAME_VERTICES = GENES_GRAPH.read.parquet("vertices")
    SAME_EDGES = GENES_GRAPH.read.parquet("edges")

    # Create an identical GraphFrame.
    SAME_GENES_GRAPH = GF.GraphFrame(SAME_VERTICES, SAME_EDGES)

# END OF FILE -------------------------------------------------------------------------------------#
