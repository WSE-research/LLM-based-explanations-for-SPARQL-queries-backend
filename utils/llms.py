import os
import random
import requests
from openai import OpenAI
from datetime import datetime
from utils.rdf import extract_entities, get_wikidata_label


MISTRAL_ENDPOINT = os.getenv("MISTRAL_ENDPOINT")
token = os.getenv("OPENAI_API_KEY")

client = OpenAI(
    api_key=token,
)

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

def find_in_cache(collection_name: str, filter_dict: dict, db):
    try:
        result = db[collection_name].find_one(filter_dict)
        if result:
            return result
        else:
            return None
    except Exception as e:
        print(str(e))
        return None

def cache_gpt(model: str, prompt: dict, result: dict, db):
    try:
        document = {
            'prompt': prompt,
            'result': result,
            'date': datetime.now()
        }
        db[model].insert_one(document)
    except Exception as e:
        print(str(e))

def ask_openai(db, prompt, model="gpt-3.5-turbo"):
  cached_result = find_in_cache(model, {"prompt": prompt}, db)
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
  cache_gpt(model, prompt, result, db)

  return result

def ask_llm(db, prompt, model="mistral-7b-finetuned"):
  cached_result = find_in_cache(model, {"prompt": prompt}, db)
  if cached_result:
    return cached_result["result"].replace("</s>", "").strip()

  r = requests.get(MISTRAL_ENDPOINT, params={"prompt": prompt})
  result = r.json()["result"]

  cache_gpt(model, prompt, result, db)

  return result.replace("</s>", "").strip()

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