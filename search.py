import os
import argparse
from dotenv import load_dotenv
from langchain_community.vectorstores.neo4j_vector import Neo4jVector
from langchain_community.embeddings import HuggingFaceInferenceAPIEmbeddings
load_dotenv()


parser = argparse.ArgumentParser()
parser.add_argument('--k', default=3)
parser.add_argument('--radius', default=1)
parser.add_argument('--query', default="israel gdp")


args = parser.parse_args()

query = args.query

radius = int(args.radius)
if radius < 1 : radius = 1

k = int(args.k)
if k < 1 : k = 1


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

embeddings = HuggingFaceInferenceAPIEmbeddings(
    api_key=HF_API_KEY,
    api_url=HF_API_URL,
    model_name="intfloat/multilingual-e5-large"
)


retrieval_query = f"""
            WITH node AS nodeEmb, score
            ORDER BY score DESC LIMIT {k}
            MATCH (nodeEmb)<-[:HAS_EMBEDDING]-(answer)
            WITH answer, score
            MATCH (d:Document)<-[:HAS_PARENT*]-(chunk:Chunk) WHERE chunk.block_idx > answer.block_idx-{radius} AND chunk.block_idx < answer.block_idx+{radius}
            WITH d, answer, chunk, score ORDER BY d.url_hash, chunk.block_idx ASC LIMIT {k*(2*radius-1)}
            WITH d, collect(answer) AS answers, collect(chunk) AS chunks, score
            RETURN {{source: d.url, page: chunks[0].page_idx+1, matched_chunk_id: id(answers[0])}} AS metadata,
                            reduce(text = "", x IN chunks | text + x.sentences + '.') AS text, score AS score LIMIT {k}
    """


existing_index_return = Neo4jVector.from_existing_index(
    embedding=embeddings,
    url=NEO4J_URL,
    username=NEO4J_USER,
    password=NEO4J_PASSWORD,
    database=NEO4J_DATABASE,
    index_name="chunkVectorIndex",
    text_node_property="key",
    retrieval_query=retrieval_query,
)



result = existing_index_return.similarity_search_with_score(query, k=k)

for doc in result:
    print(doc)