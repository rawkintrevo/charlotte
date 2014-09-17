__author__ = 'Trevor Danger Grant'

from googleads import adwords
from pymongo import MongoClient, errors
from progressbar import Percentage, Bar, RotatingMarker, ETA, FileTransferSpeed, ProgressBar

DB= 'example'
COLLECTION= 'example1'
SEED_KEYWORDS = ['pizza', 'velvet underground']
TOTAL_DAILY_OPS = 9950

PAGE_SIZE= 99





############################################################################
### Establish Nessicary Connections
client = MongoClient()
db = client[DB]
col = db[COLLECTION]

client = adwords.AdWordsClient.LoadFromStorage('googleads.yaml')

widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker()),
           ' ', ETA(), ' ', FileTransferSpeed()]


#############################################################################
### Special Wrapper Function

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


total_counter = 0

loop_counter= 0
while TOTAL_DAILY_OPS > total_counter:
	if loop_counter == 0:pbar = ProgressBar(widgets=widgets, maxval=TOTAL_DAILY_OPS).start()
	total_counter += 1
	pbar.update(total_counter+1)
	seed_keyword = SEED_KEYWORDS.pop(0)
	new_docs = []
	seed_doc = {'KEYWORD_TEXT':seed_keyword, 'EDGES':[] }
	rank = 0
	page = fetch_seed_ideas(seed_keyword, PAGE_SIZE)
	for entry in page[1]:
		temp_doc = {}
		rank += 1
		for item in entry.data:
			key = item.key
			value = getattr(item.value , 'value', 0)
			temp_doc[key] = value
			if key == 'KEYWORD_TEXT':
				seed_doc['EDGES'].append((value, rank))
		orig_doc = col.find_one({'KEYWORD_TEXT':temp_doc['KEYWORD_TEXT']})
		if orig_doc is None:
			new_docs.append(temp_doc)
			if not temp_doc['KEYWORD_TEXT'] in SEED_KEYWORDS:
					SEED_KEYWORDS.append(temp_doc['KEYWORD_TEXT'])
	orig_doc = col.find_one({'KEYWORD_TEXT':seed_keyword})
	if orig_doc is None:
		new_docs.append(seed_doc)
	else:
		dummy= col.update({"KEYWORD_TEXT": seed_keyword},{"$set": {'EDGES': seed_doc['EDGES']}})
	if len(new_docs) > 0:
		dummy = col.insert(new_docs)





			
		

