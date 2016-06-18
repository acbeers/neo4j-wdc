Tableau Web Data Connector for Neo4J
====================================

This a very simple connector for Neo4J for Tableau, using the Web Data Connector 2.0 API.  It supports authentication, running Cypher queries, and pulling down full collections of nodes by labels.  If multiple labels are selected, or labels and a Cypher query, then multiple tables are returned to Tableau.

Things left to do:

- support multiple Cypher queries
- improve the visual design
- add better error handling
- potentially add additional non-query based ways of pulling data
- break the code up into smaller pieces, add testing

Limitations:

- for queries that return multiple nodes, only the first node is extracted.  Example:
  - match (e)-[:reportsto]->(m) return e,m
