from gexf import Gexf
from py2neo import cypher, neo4j

__author__ = 'Trevor Grant'


# test helloworld.gexf
gexf = Gexf(__author__,__title__)
graph=gexf.addGraph("directed","static",__desc__)
graph_db = neo4j.GraphDatabaseService()


query = neo4j.CypherQuery(graph_db, "MATCH (n) WHERE n.on_path=1 RETURN n" ) 
results = query.execute()
# Query for relationships where both nodes have .on_path
for r in results:
	quite =graph.addNode(str(r[0]._id),r[0]['KEYWORD_TEXT'])
#	if r[0]._id == 2418: print 'yup'


RESULTS_PER_PAGE= 1000
# Query for nodes with .on_path

paging = False
skip=0
query = neo4j.CypherQuery(graph_db, "MATCH (m {on_path:1})-[r]->(n {on_path:1}) RETURN r ORDER BY ID(r) SKIP %i LIMIT %i" % (skip,RESULTS_PER_PAGE ))
results = query.execute()
if len(results)==1000: 
	paging=True
	skip += RESULTS_PER_PAGE
# Query for relationships where both nodes have .on_path

for r in results:
	quiet= graph.addEdge(str(r[0]._id),str(r[0].start_node._id),str(r[0].end_node._id))
while paging:
	query = neo4j.CypherQuery(graph_db, "MATCH (m {on_path:1})-[r]->(n {on_path:1}) RETURN r ORDER BY ID(r) SKIP %i LIMIT %i" % (skip,RESULTS_PER_PAGE ))
	results = query.execute()
	if len(results)==1000: 
		paging=True
		skip += RESULTS_PER_PAGE
	else: paging = False	


#	r[0]._id #id
#	r[0].get_properties()  # get's properties
#	r[0].end_node			# id of TO node
#	r[0].start_node			# id of FROM node
#	


output_file=open("Path Example.gexf","w")
gexf.write(output_file)


