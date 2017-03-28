# Neo4J: Useful Cypher QUERIES
## Viewing the graph

```SQL
MATCH (n)-[r]->(m)
RETURN n, r, m
```

## Finding the shortest path between two nodes (officers/companies)

```SQL
MATCH p=shortestPath((a:Officer)-[*]-(c:Company))
WHERE a.id='ThPiUKZofMPwxbf-CSpfKLZkoTI' and c.id='09321129'
RETURN p, length(p), 
LIMIT 1
```

## Get all shortest paths between two nodes (officers/companies)

```SQL
MATCH p = allShortestPaths((source)-[*]-(destination)) 
WHERE source.id='ThPiUKZofMPwxbf-CSpfKLZkoTI' AND destination.id ='09321129' 
RETURN EXTRACT(n IN NODES(p)| n.name) AS Paths, length(p) AS Hops
ORDER BY Hops DESC
```

## All Shortest Paths with Path Conditions:

```SQL
MATCH p = allShortestPaths((source)-[*]-(destination))
WHERE source.id='26hN8qN8IWGvExs0_vuW9cMSDn8' AND destination.id = '09321085' AND LENGTH(NODES(p)) > 3
RETURN EXTRACT(n IN NODES(p)| n.name) AS Paths,length(p)
```

## Diameter of the graph:

```SQL
MATCH (n), (m)
WHERE n <> m
WITH n, m
MATCH p=shortestPath((n)-[*]-(m))
RETURN n.name, m.name, length(p)
ORDER BY length(p) desc limit 1
```

## Graph not containing a selected neighborhood:

```SQL
MATCH (a {name: 'Daniele ANGELUCCI'})-[*..1]-(b)
WITH collect(distinct b.name) as MyList
MATCH (n)-[*]->(m)
WHERE not(n.name in MyList) and not (m.name in MyList)
RETURN distinct n, m
```