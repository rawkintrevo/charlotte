__author__ = 'Trevor Danger Grant'

from googleads import adwords
from py2neo import neo4j, node, rel
from progressbar import Percentage, Bar, RotatingMarker, ETA, ProgressBar
from datetime import datetime, timedelta
from time import sleep

DB= 'example'
COLLECTION= 'example1'
SEED_KEYWORDS = ['pizza', 'velvet underground']

ADWORDS_OPS_LIMIT = 1000  # True Limit = 10,000, but you probably want to recock after 1000 or so with some new seedwords

GENERATIONS = 5
KEYWORDS_PER_GENERATION = 30

MIN_DAYS_BEFORE_UPDATE = 30
PAGE_SIZE= 99		### NOTE:  99 is the maximum idea you can have and still count it as 1 request.
adwords_ops_count = 0  # counter, don't actually reset this

### This bloc will find early terminated nodes and use them as a basis for SEED_KEYWORDS
#graph_db = neo4j.GraphDatabaseService()
#query = neo4j.CypherQuery(graph_db, "match (n)--() with n,count(*) as rel_cnt where rel_cnt < 10 return n limit 750")
#new_seeds = query.execute()

#for seed in new_seeds.data:
#	n, = seed.values
#	SEED_KEYWORDS.append(n['KEYWORD_TEXT'])

	


############################################################################
### Establish Nessicary Connections

# Graph Service
graph_db = neo4j.GraphDatabaseService()

client = adwords.AdWordsClient.LoadFromStorage('googleads.yaml')

#############################################################################
### Special Wrapper Functions

def fetch_seed_ideas(SEED_KEYWORD, PAGE_SIZE= 25 ):
	targeting_idea_service = client.GetService(
      'TargetingIdeaService', version='v201406')
	selector = {
		'searchParameters': [{
		      'xsi_type': 'RelatedToQuerySearchParameter',
		      'queries': SEED_KEYWORD,
		  }] ,
		'ideaType': 'KEYWORD' ,
		'requestType': 'IDEAS',
		'requestedAttributeTypes': [	'KEYWORD_TEXT',	'SEARCH_VOLUME', 'COMPETITION'],
		'paging': {	'startIndex': str(0),  'numberResults': PAGE_SIZE},	
		'localeCode': 'en_US' 
	}
	page = targeting_idea_service.get(selector)
	return page


def progress_bar_stuff(pbar, adwords_ops_count, ADWORDS_OPS_LIMIT):
	if pbar is None: 
		widgets = ['Adwords Ops Used: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
           ' ', ETA(), ' ']
		pbar = ProgressBar(widgets=widgets, maxval=ADWORDS_OPS_LIMIT).start()
	adwords_ops_count += 1
	pbar.update(adwords_ops_count)
	return pbar, adwords_ops_count
	

def get_or_create_node(graph_db, KEY, VALUE, FULL_NODE={}):
	query = neo4j.CypherQuery(graph_db, "MATCH (a) WHERE a.%s = '%s' RETURN a" % (KEY,VALUE) ) 
	results = query.execute()
	if len(results.data)==1:
		d, = results.data[0].values
		return d
	elif len(results.data)==0:
		if len(FULL_NODE)==0: a, = graph_db.create(node({KEY: VALUE}) )
		if len(FULL_NODE) > 0: a, = graph_db.create(node(FULL_NODE) )
		return a
	elif len(results.data) > 1:
		return False
	else:
		raise Exception

#### On the first pass you just give a couple of SEED_KEYWORDS
#### Then, in later passes you do a query to find all of the non-terminal nodes



pbar = None
last_fetch = datetime.now()
while (ADWORDS_OPS_LIMIT > adwords_ops_count) and (len(SEED_KEYWORDS) > 0):
	pbar, adwords_ops_count = progress_bar_stuff(pbar, adwords_ops_count, ADWORDS_OPS_LIMIT)
	seed_keyword = SEED_KEYWORDS.pop(0)	## Get the first keyword in the seed list
	while datetime.now() - last_fetch < timedelta(0,30):
		sleep(1)
	fetch = fetch_seed_ideas(seed_keyword, PAGE_SIZE)	## These two lines nessicary. 
	if len(fetch) > 1: adwords_page = fetch[1]			## If there are no enteries fetch[1] is out of index
	last_fetch= datetime.now()
	seed_node = get_or_create_node(graph_db,'KEYWORD_TEXT',seed_keyword)
	rank = 0
	for entry in adwords_page:
		temp_doc = {}
		rank += 1
		for item in entry.data:
			key = item.key
			value = getattr(item.value , 'value', 0)
			if isinstance(value, unicode):
				temp_doc[key] = value.encode('ascii','ignore')
			else: temp_doc[key] = value
		if bool(temp_doc['KEYWORD_TEXT'].strip()):
			new_node = get_or_create_node(graph_db, 'KEYWORD_TEXT', temp_doc['KEYWORD_TEXT'], temp_doc)
			if 'last_updated' in new_node.get_properties().keys():
				last_update = new_node['last_updated']
				if (datetime.now() - last_update)  > timedelta(MIN_DAYS_BEFORE_UPDATE = 30):
					SEED_KEYWORDS.append(new_node['KEYWORD_TEXT'])
					new_node['last_update'] = datetime.now()
			else:
				SEED_KEYWORDS.append(new_node['KEYWORD_TEXT'])
				new_node['last_update'] = datetime.now()
			dummy = graph_db.create(rel(seed_node, "LEADS_TO", new_node, {"rank":rank}) )
		else:
			pass  ### This means that the KEYWORD_TEXT was an empty string



		
	

