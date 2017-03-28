# Genes Association Graph

Below you can see a snapshot of the graph in Neo4j. You can load the graph with:

```SQL
LOAD CSV WITH HEADERS FROM "file:///gene_gene_associations_50k.csv" AS line
MERGE (n:Gene {Name:line.OFFICIAL_SYMBOL_A})
MERGE (m:Gene {Name:line.OFFICIAL_SYMBOL_B})
MERGE (n) -[:ASSOCIATED_WITH {Type:line.EXPERIMENTAL_SYSTEM}]-> (m)
```

and to see the following snapshot type:

```MySQL
MATCH (n)-[*1..5]-(m)
RETURN n,m
LIMIT 1000
```

![The Genes Association Graph](https://github.com/DataReplyUK/datareplyuk/blob/master/GenesAssociation/data/graph.png)

## License

(c) Copyright 2016 Data Reply UK, all rights reserved.
