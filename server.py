import os
import json
import random
import uvicorn
import logging
import requests
from time import sleep
from rdflib import URIRef
from openai import OpenAI
from fastapi import FastAPI
from datetime import datetime
from pymongo import MongoClient
from pyparsing import ParseResults
from rdflib.plugins.sparql.parser import parseQuery


__version__ = "0.1.1"

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

headers = {
    'User-Agent': 'wiki_parser_online/0.17.1 (https://deeppavlov.ai; info@deeppavlov.ai) deeppavlov/0.17.1',
}

WIKIDATA = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

ZERO_SHOT_PROMPT = {
    'type': "ZERO_SHOT",
    'en': {
        'head': 'Having a SPARQL query: {query} \n Where:\n ',
        'list': '{uri} has human-readable name "{uriLabel}."',
        'tail': '\n Transform the SPARQL query to a natural language question. Output just the transformed question'
    },
    'ru': {
        'head': 'Имея следующий SPARQL запрос: {query} \n Где:\n ',
        'list': '{uri} именуется как "{uriLabel}."',
        'tail': '\n Трансформируй SPARQL запрос в вопрос на естественном языке. Выведи только транфсормируемый вопрос'
    },
    'de': {
        'head': 'Gegeben ist die SPARQL-Anfrage: {query} \n Dabei gilt:\n ',
        'list': 'Die Bezeichnung von {uri} ist "{uriLabel}."',
        'tail': '\n Transformiere die SPARQL-Anfrage in eine Frage in natürlicher Sprache. Gib nur die transformierte Frage aus.'
    }
}

ONE_SHOT_PROMPT = {
    'type': "ONE_SHOT",
    'en': {
        'shot': "---- Start Example ---- \n {shot} \n ----End Example ---- \n",
        'head': 'Having a SPARQL query: {query} \n Where:\n ',
        'list': '{uri} has human-readable name "{uriLabel}."',
        'tail': '\n Transform the SPARQL query to a natural language question. Output just the transformed question'
    },
    'ru': {
        'shot': "{shot} \n ----------- \n",
        'head': 'Имея следующий SPARQL запрос: {query} \n Где:\n ',
        'list': '{uri} именуется как "{uriLabel}."',
        'tail': '\n Трансформируй SPARQL запрос в вопрос на естественном языке. Выведи только транфсормируемый вопрос'
    },
    'de': {
        'shot': "{shot} \n ----------- \n",
        'head': 'Gegeben ist die SPARQL-Anfrage: {query} \n Dabei gilt:\n ',
        'list': 'Die Bezeichnung von {uri} ist "{uriLabel}."',
        'tail': '\n Transformiere die SPARQL-Anfrage in eine Frage in natürlicher Sprache. Gib nur die transformierte Frage aus.'
    }
}

with open("qald.json") as f:
    dataset = json.load(f)["questions"]

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)

app = FastAPI()

token = os.getenv("OPENAI_API_KEY")

client = OpenAI(
    api_key=token,
)

mongo_client = MongoClient(f"{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}",
    username=os.getenv("MONGO_USERNAME"),
    password=os.getenv("MONGO_PASSWORD"),
    authSource='admin'
)

db = mongo_client['SPARQL2NL']

def find_in_cache(collection_name: str, filter_dict: dict):
    try:
        result = db[collection_name].find_one(filter_dict)
        if result:
            return result
        else:
            return None
    except Exception as e:
        print(str(e))
        return None

def cache_gpt(model: str, prompt: dict, result: dict):
    try:
        document = {
            'prompt': prompt,
            'result': result,
            'date': datetime.now()
        }
        db[model].insert_one(document)
    except Exception as e:
        print(str(e))

def ask_openai(prompt, model="gpt-3.5-turbo"):
  cached_result = find_in_cache(model, {"prompt": prompt})
  if cached_result:
    return cached_result["result"]

  chat_completion = client.chat.completions.create(
      messages=[
          {
              "role": "user",
              "content": prompt,
          }
      ],
      model=model,
  )

  result = chat_completion.choices[0].message.content
  cache_gpt(model, prompt, result)

  return result

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
        r = requests.get(endpoint_url, headers=headers, params = {'format': 'json', 'query': query})

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

def get_question_by_language(item, dataset='qald', lang='en'):
  q_list = []
  if dataset == 'qald':
    for q in item['question']:
      if q['language'] == lang:
        q_list.append(q['string'])
    return q_list
  elif dataset == 'rubq':
    if lang == 'en':
      q_list.append(item["question_eng"])
    elif lang == 'ru':
      q_list.append(item["question_text"])
    else:
      assert False
    return q_list
  else:
    assert False

def make_the_prompt(cache, query, prompt_template, lang='en', dataset=None):
    entities = extract_entities(query)

    list_labels = []
    for i in entities:
        label = get_wikidata_label(cache, i, lang)
        if label:
            list_labels.append(prompt_template[lang]['list'].format(uri=i, uriLabel=label))

    if prompt_template["type"] == "ZERO_SHOT":
      prompt = prompt_template[lang]["head"].format(query=query) + "\n ".join(l for l in list_labels) + prompt_template[lang]["tail"]
    elif prompt_template["type"] == "ONE_SHOT":
      queries = [q["query"]["sparql"] for q in dataset].copy()
      questions = [get_question_by_language(q, 'qald', lang) for q in dataset].copy()

      if query in queries:
        r_idx = queries.index(query)
        queries.pop(r_idx) # remove the main query
        questions.pop(r_idx) # remove the main query

      c_idx = random.choice([i for i in range(len(queries))]) # choice a random query to make a one shot
      shot_query = queries[c_idx]
      gold_standard = questions[c_idx]

      shot = make_the_prompt(cache, shot_query, {"type": "ZERO_SHOT", lang: prompt_template[lang]}, lang=lang)
      shot += ". " + random.choice(gold_standard)
      prompt = prompt_template[lang]["shot"].format(shot=shot) + prompt_template[lang]["head"].format(query=query) + "\n ".join(l for l in list_labels) + prompt_template[lang]["tail"]

    return prompt

@app.get("/explanation")
async def root(query_text: str, language: str = "en", shots: int = 1, model: str = "gpt-4-1106-preview"):
    logger.info(query_text)
    try:
        if shots > 1 or shots < 0:
            return {"message": "Invalid number of shots. It should be either 0 or 1."}, 500
        if language not in ["en", "ru", "de"]:
            return {"message": "Invalid language. It should be either en, de, ru."}, 500
        if len(query_text) == 0:
            return {"message": "Invalid query. It should not be empty."}, 500

        if shots == 0:
            prompt_template = ZERO_SHOT_PROMPT
        elif shots == 1:
            prompt_template = ONE_SHOT_PROMPT

        prompt = make_the_prompt(db, query_text, prompt_template, language, dataset)
        predicted_nl = ask_openai(prompt=prompt, model=model)

        return {"explanation": predicted_nl}, 200
    except Exception as e:
        return {"message": str(e)}, 500

@app.post("/feedback")
async def feedback(payload: dict):
    try:
        query = payload.get('query_text', '')
        verbalization = payload.get('verbalization', '')
        rating = payload.get('rating', 0)
        comment = payload.get('comment', '')

        if rating < 1 or rating > 5:
            return {"message": "Invalid rating. It should be between 1 and 5."}, 400
        if len(query) == 0 or len(verbalization) == 0:
            return {"message": "Invalid query or verbalization. They should not be empty."}, 400

        feedback = {
            'query': query,
            'verbalization': verbalization,
            'rating': rating,
            'comment': comment,
            'date': datetime.now()
        }
        db['feedback'].insert_one(feedback)

        return {"message": "Feedback received successfully."}, 200
    except Exception as e:
        return {"message": str(e)}, 500

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
