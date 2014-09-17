from py2neo import cypher, neo4j
from progressbar import Percentage, Bar, RotatingMarker, ETA, ProgressBar

session = cypher.Session('http://localhost:7474')

source = 2389
end = 5116
widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
           ' ', ETA(), ' ']

## OPTIONS:  Stop when path found,  Stop at depth Found, Full Depth


#def Dijkstra(graph_db, source):  # source id#

for prop in ['dist', 'on_path']:
	tx = session.create_transaction()
	tx.append("MATCH (n) WHERE HAS(n.%s) REMOVE n.%s" % (prop, prop))
	dummy= tx.commit()


### Source Node
tx = session.create_transaction()
tx.append("start n = node(%i) SET n.dist=0" % source)
dummy= tx.commit()


for dist in range(0,11):
	tx = session.create_transaction()
	tx.append("MATCH ((n {dist:%i})-[:LEADS_TO*..1]-(m)) WHERE m.dist IS NULL RETURN count(*)" % dist)
	count, = tx.commit()[0][0].values
	print "%i: %i" % (dist,count)
	if count > 0:
		tx = session.create_transaction()
		tx.append("MATCH ((n {dist:%i})-[:LEADS_TO*..1]-(m)) WHERE m.dist IS NULL SET m.dist=%i" % (dist, dist+1) )
		dummy = tx.commit()
	else: break


graph_db = neo4j.GraphDatabaseService()
query = neo4j.CypherQuery(graph_db, "START a= node(%i) RETURN a" %end) 
results = query.execute()
end_node, = results[0].values

if 'dist' in end_node.get_properties():
	print 'Success: Node %i found in network with Node %i, %i layers away' % (end, source, end_node['dist'])

tx = session.create_transaction()
tx.append("START n= node(%i) SET n.on_path=1" % (end) )
results= tx.commit()

max_dist= dist ## How Far did Loop 1 run?
for back_dist in range(max_dist):
	tx = session.create_transaction()
	tx.append('MATCH (from)-[LEADS_TO]->(to) WHERE to.on_path=1 AND from.dist <= to.dist  SET from.on_path=1')	
	dummy= tx.commit()

tx = session.create_transaction()
tx.append("MATCH (n) WHERE n.on_path = 1 RETURN count(*)")
count, = tx.commit()[0][0].values


tx = session.create_transaction()
tx.append("MATCH (n) WHERE n.on_path = 1 RETURN n")
results = tx.commit()

