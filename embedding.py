import os
from dotenv import load_dotenv
from neo4j import GraphDatabase

import requests

load_dotenv()

NEO4J_URL = os.getenv('NEO4J_URL')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
NEO4J_DATABASE = os.getenv('NEO4J_DATABASE')
HF_API_URL = os.getenv('HF_API_URL')
HF_API_KEY = os.getenv('HF_API_KEY')

headers = {
	"Accept" : "application/json",
	"Authorization": "Bearer " + HF_API_KEY,
	"Content-Type": "application/json"
}

def query(payload):
	response = requests.post(HF_API_URL, headers=headers, json=payload)
	return response.json()


def get_embedding(client, text, model):
    response = client.embeddings.create(
                    input=text,
                    model=model,
                )
    return response.data[0].embedding

def LoadEmbedding(label, property):
    driver = GraphDatabase.driver(NEO4J_URL, auth=(NEO4J_USER, NEO4J_PASSWORD), database=NEO4J_DATABASE)


    with driver.session() as session:
        # get chunks in document, together with their section titles
        # result = session.run(f"MATCH (ch:{label}) -[:HAS_PARENT]-> (s:Section) RETURN id(ch) AS id, s.title + ' >> ' + ch.{property} AS text")
        result = session.run(f"MATCH (ch:{label}) -[:HAS_PARENT]-> (s:Document) WHERE NOT (ch)-[:HAS_EMBEDDING]-() RETURN id(ch) AS id, s.url + ' >> ' + ch.{property} AS text")
        # call  embedding API to generate embeddings for each proporty of node
        # for each node, update the embedding property
        count = 0
        for record in result:
            id = record["id"]
            text = record["text"]

            # For better performance, text can be batched
            embeddings = query({
                "inputs": text,
                "parameters": {}
            })

            embedding = embeddings[0]

            # key property of Embedding node differentiates different embeddings
            cypher = "CREATE (e:Embedding) SET e.key=$key, e.value=$embedding"
            cypher = cypher + " WITH e MATCH (n) WHERE id(n) = $id CREATE (n) -[:HAS_EMBEDDING]-> (e)"
            session.run(cypher,key=property, embedding=embedding, id=id )
            count = count + 1

        session.close()

        print("Processed " + str(count) + " " + label + " nodes for property @" + property + ".")
        return count

LoadEmbedding("Chunk", "sentences")
LoadEmbedding("Table", "name")