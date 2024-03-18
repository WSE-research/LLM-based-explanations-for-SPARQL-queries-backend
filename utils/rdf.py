import requests
from time import sleep
from utils import logger
from rdflib import URIRef
from pyparsing import ParseResults
from rdflib.plugins.sparql.parser import parseQuery


WIKIDATA = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

WIKIDATA_PREFIXES = ['wd:', 'wdt:', 'p:', 'ps:', 'pq:',]

FIXED_LABELS = {
    'rdfs:label': 'label',
    'http://www.w3.org/2000/01/rdf-schema#label': 'label',
    'skos:altLabel': 'alternative label',
    'xsd:integer': 'integer',
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': 'type',
    'http://www.w3.org/2001/XMLSchema#integer': 'integer',
    'http://www.w3.org/2001/XMLSchema#gYear': 'year',
    'http://www.w3.org/2001/XMLSchema#double': 'double',
}

def extract_entities_recursive(parsed):
    results = []

    if isinstance(parsed, URIRef):
        results += [str(parsed)]

    if isinstance(parsed, (list, ParseResults)):
        for i in list(parsed):
            results += extract_entities_recursive(i)

    if isinstance(parsed, dict):
        if 'prefix' in parsed and 'localname' in parsed:
            results += [f'{parsed["prefix"]}:{parsed["localname"]}']
        else:
            for i in parsed.values():
                results += extract_entities_recursive(i)

    return results

def extract_entities(sparql):
    try:
        return extract_entities_recursive(parseQuery(sparql)[1]['where'])
    except:
        return []

def execute(query: str, endpoint_url: str = WIKIDATA):
    """
    Send query direct to wikidata.

    query: SPARQL query
    endpoint_url: endpoint of wikidata query service

    execute(): json response
    """
    try:
        r = requests.get(endpoint_url, params = {'format': 'json', 'query': query})

        if r.status_code == 200:
            return r.json()

    except Exception as e:
        logger.error(f'Exception in function "execute": {e}')

    return None

def query_wikidata(query: str, repeat: int = 3, timeout: float = 10.0):
    """
    Send query direct to wikidata.
    query: SPARQL query
    query_wikidata(): list of dicts
    """
    while repeat > 0:
        try:
            return execute(query)['results']['bindings']

        except Exception as e:
            logger.error(f'Exception in function "query_wikidata": {e}')
            sleep(timeout)
            repeat -= 1

    return None

def query_wikidata_label(uri: str, lang: str='en') -> str:
    query = (
        'SELECT ?label WHERE {',
        '  {',
        f'    {uri} rdfs:label ?label .',
        '  }',
        '  UNION',
        '  {',
        f'    {uri} owl:sameAs+ ?redirect .',
        '    ?redirect rdfs:label ?label .',
        '  }',
        f'  FILTER (lang(?label) = "{lang}")',
        '} LIMIT 1'
    )

    try:
        data = query_wikidata('\n'.join(query))
        return data[0]['label']['value']
    except KeyError:
        raise
    except Exception as e:
        # print(f'Exception in function "query_wikidata_label": {e}')
        # print(f'Input parameters: {uri}, {lang}')
        return None


def get_wikidata_label_cached(cache, uri: str, lang: str='en') -> str:
    label = cache.wikidata_labels.find_one({'uri': uri, 'lang': lang})

    if label:
        logger.debug(f"Found label in cache {label}")
        return label['label']

    prefix = 'wd' # it works despite property or entity

    try:
        label = query_wikidata_label(f'{prefix}:{uri}', lang)
        logger.debug(f"get_wikidata_label_cached label: {str(label)}, lang: {str(lang)}")
        if not label:
            label = None

        cache.wikidata_labels.insert_one({ 'uri': uri, 'lang': lang, 'label': label })

    except KeyboardInterrupt:
        raise
    except Exception as e:
        # print(f'Exception in function "get_wikidata_label_cached": {e}')
        # print(f'Input parameters: {uri}, {lang}')
        return None
    logger.debug(f"returning label {label} from  get_wikidata_label_cached")

    return label

def get_wikidata_label(cache, literal, lang='en'):
    literal = literal.strip('(<>).')

    if literal in FIXED_LABELS:
        return FIXED_LABELS[literal]

    if not any(i in literal for i in WIKIDATA_PREFIXES+['http']):
        if "xsd:" not in literal:
            return literal
        else:
            return None

    if any(literal.startswith(i) for i in WIKIDATA_PREFIXES):
        w_id = literal.split(':')[-1]
    else:
        parts = literal.split('/')
        w_id = parts[-1][:-1] if parts[-1].endswith(">") else parts[-1]

    label = get_wikidata_label_cached(cache, w_id, lang)

    return label