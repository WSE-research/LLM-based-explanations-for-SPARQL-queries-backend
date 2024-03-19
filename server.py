import os
import json
import uvicorn
from utils import logger
from fastapi import FastAPI, Request
from datetime import datetime
from pymongo import MongoClient
from fastapi import Header, HTTPException
from utils.llms import ZERO_SHOT_PROMPT, ONE_SHOT_PROMPT, make_the_prompt, ask_openai


# Load environment variables from .env file if it exists
if os.path.exists('.env'):
    from dotenv import load_dotenv
    load_dotenv('.env')

with open("qald.json") as f:
    dataset = json.load(f)["questions"]

app = FastAPI()

mongo_client = MongoClient(f"{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}",
    username=os.getenv("MONGO_USERNAME"),
    password=os.getenv("MONGO_PASSWORD"),
    authSource='admin'
)

db = mongo_client['SPARQL2NL']

@app.get("/explanation")
async def root(request: Request, query_text: str, language: str = "en", shots: int = 1, model: str = "gpt-4-1106-preview"):
    x_custom_header = request.headers.get('X-Custom-Header')

    logger.info(str(request.headers))

    if x_custom_header != os.getenv("SECURITY_HEADER_VALUE"):
        raise HTTPException(status_code=400, detail="X-Custom-Header not found or invalid")
    
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
        predicted_nl = ask_openai(db=db, prompt=prompt, model=model)

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
