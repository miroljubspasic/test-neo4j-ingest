import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
import hashlib
from unstructured.partition.api import partition_via_api

load_dotenv()

NEO4J_URL = os.getenv('NEO4J_URL')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
NEO4J_DATABASE = os.getenv('NEO4J_DATABASE')
UNSTRUCTURED_API_URL=os.getenv('UNSTRUCTURED_API_URL')


####
#### PDF -> NEO4J


def initialiseNeo4j():
    cypher_schema = [
        "CREATE CONSTRAINT sectionKey IF NOT EXISTS FOR (c:Section) REQUIRE (c.key) IS UNIQUE;",
        "CREATE CONSTRAINT chunkKey IF NOT EXISTS FOR (c:Chunk) REQUIRE (c.key) IS UNIQUE;",
        "CREATE CONSTRAINT documentKey IF NOT EXISTS FOR (c:Document) REQUIRE (c.url_hash) IS UNIQUE;",
        "CREATE CONSTRAINT tableKey IF NOT EXISTS FOR (c:Table) REQUIRE (c.key) IS UNIQUE;",
        "CREATE CONSTRAINT elementKey IF NOT EXISTS FOR (c:Element) REQUIRE (c.key) IS UNIQUE;",
        "CALL db.index.vector.createNodeIndex('chunkVectorIndex', 'Embedding', 'value', 1536, 'COSINE');"
    ]

    driver = GraphDatabase.driver(NEO4J_URL, database=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        for cypher in cypher_schema:
            session.run(cypher)
    driver.close()

def ingestDocumentNeo4j(elements, doc_location):

    cypher_pool = [
        # 0 - Document
        "MERGE (d:Document {url_hash: $doc_url_hash_val}) ON CREATE SET d.url = $doc_url_val, d.last_modified = $doc_last_modified_val RETURN d;",
        # 1 - Section
        "MERGE (p:Section {key: $element_id_val}) ON CREATE SET p:Element, p.page_idx = $page_idx_val, p.title_hash = $title_hash_val, p.block_idx = $block_idx_val, p.title = $title_val, p.tag = $tag_val RETURN p;",
        # 2 - Link Section with the Document
        "MATCH (d:Document {url_hash: $doc_url_hash_val}) MATCH (s:Section {key: $element_id_val}) MERGE (d)<-[:HAS_DOCUMENT]-(s);",
        # 3 - Link Section with a parent Element
        "MATCH (s1:Section {key: $element_id_val}) MATCH (s2:Element {key: $sec_parent_element_id_val}) MERGE (s2)<-[:UNDER_SECTION]-(s1);",
        # 4 - Chunk
        "MERGE (c:Chunk {key: $element_id_val}) ON CREATE SET c:Element, c.sentences = $sentences_val, c.sentences_hash = $sentences_hash_val, c.block_idx = $block_idx_val, c.page_idx = $page_idx_val, c.tag = $tag_val RETURN c;",
        # 5 - Link Chunk to another element
        "MATCH (c:Chunk {key: $element_id_val}) MATCH (s:Element {key:$chk_parent_element_id_val}) MERGE (s)<-[:HAS_PARENT]-(c);",
        # 6 - Table
        "MERGE (t:Table {key: $element_id_val}) ON CREATE SET t:Element, t.name = $name_val, t.doc_url_hash = $doc_url_hash_val, t.block_idx = $block_idx_val, t.page_idx = $page_idx_val, t.html = $html_val, t.rows = $rows_val RETURN t;",
        # 7 - Link Table to Section
        "MATCH (t:Table {key: $element_id_val}) MATCH (s:Section {key: $tb_parent_element_id_val}) MERGE (s)<-[:HAS_PARENT]-(t);",
        # 8 - Link Table to Document
        "MATCH (t:Table {key: $element_id_val}) MATCH (s:Document {url_hash: $doc_url_hash_val}) MERGE (s)<-[:HAS_PARENT]-(t);",
        # 9 - Image
        "MERGE (t:Image {key: $element_id_val}) ON CREATE SET t:Element, t.name = $name_val, t.doc_url_hash = $doc_url_hash_val, t.block_idx = $block_idx_val, t.page_idx = $page_idx_val RETURN t;",
        # 10 - Link Image to Document
        "MATCH (t:Image {key: $element_id_val}) MATCH (s:Document {url_hash: $doc_url_hash_val}) MERGE (s)<-[:HAS_PARENT]-(t);",
        # 11 - Link top Chunk to Document
        "MATCH (t:Chunk {key: $element_id_val}) MATCH (s:Document {url_hash: $doc_url_hash_val}) MERGE (s)<-[:HAS_PARENT]-(t);"
    ]

    driver = GraphDatabase.driver(NEO4J_URL, database=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        cypher = ""

        # 1 - Create Document node
        doc_url_val = doc_location
        doc_url_hash_val = hashlib.md5(doc_url_val.encode("utf-8")).hexdigest()
        doc_last_modified_val = elements[0].metadata.last_modified

        cypher = cypher_pool[0]
        session.run(cypher, doc_url_hash_val=doc_url_hash_val, doc_url_val=doc_url_val, doc_last_modified_val=doc_last_modified_val)

        # 2 - Create Section nodes if element.category = 'Title'

        countSection = 0
        countChunk = 0
        countTable = 0
        countImage = 0

        # iterate all items in list elements and keep an index i
        for i, sec in enumerate(elements) :

            tag_val = sec.category
            page_idx_val = sec.metadata.page_number
            block_idx_val = i
            element_id_val = sec.id
            text_val = sec.text
            text_hash_val = hashlib.md5(text_val.encode("utf-8")).hexdigest()
            parent_id_val = str(sec.metadata.parent_id)

            if sec.category == 'Title':

                # MERGE section node
                cypher = cypher_pool[1]
                session.run(cypher, page_idx_val=page_idx_val
                                    , title_hash_val=text_hash_val
                                    , title_val=text_val
                                    , tag_val=tag_val
                                    , block_idx_val=block_idx_val
                                    , doc_url_hash_val=doc_url_hash_val
                                    , element_id_val=element_id_val
                        )

                # Link Section with a parent section or Document

                if parent_id_val == "None":    # use Document as parent
                    cypher = cypher_pool[2]
                    session.run(cypher
                                        , doc_url_hash_val=doc_url_hash_val
                                        , element_id_val=element_id_val
                        )

                else:   # use parent section
                    cypher = cypher_pool[3]
                    session.run(cypher
                                        , sec_parent_element_id_val=parent_id_val
                                        , doc_url_hash_val=doc_url_hash_val
                                        , element_id_val=element_id_val
                                )
                # **** if sec_parent_val == "None":

                countSection += 1
                continue
            # **** for sec in elements: category = 'Title'


        # ------- Continue within the session block -------
        # 3 - Create Chunk nodes from chunks

            if sec.category == 'NarrativeText' or sec.category == 'List' or sec.category == 'ListItem' \
                or sec.category == 'UncategorizedText' or sec.category == 'Header':


                # MERGE chunk node
                cypher = cypher_pool[4]
                session.run(cypher, sentences_hash_val=text_hash_val
                                    , sentences_val=text_val
                                    , block_idx_val=block_idx_val
                                    , page_idx_val=page_idx_val
                                    , tag_val=tag_val
                                    , doc_url_hash_val=doc_url_hash_val
                                    , element_id_val=element_id_val
                            )

                # Link chunk with a parent Element. If none, link it to Document

                if not parent_id_val == "None":

                    cypher = cypher_pool[5]
                    session.run(cypher
                                    , doc_url_hash_val=doc_url_hash_val
                                    , chk_parent_element_id_val=parent_id_val
                                    , element_id_val=element_id_val
                                )
                else:   # link chunk to Document
                    cypher = cypher_pool[11]
                    session.run(cypher
                                    , doc_url_hash_val=doc_url_hash_val
                                    , element_id_val=element_id_val
                                )

                countChunk += 1
                continue
            # **** for sec in elements: Chunk

            # 4 - Create Table nodes

            if sec.category == 'Table':

                html_val = sec.metadata.text_as_html
                # count <tr> in html
                rows_val = len(html_val.split('</tr>'))

                # MERGE table node

                cypher = cypher_pool[6]
                session.run(cypher, block_idx_val=block_idx_val
                                , page_idx_val=page_idx_val
                                , name_val=text_val
                                , html_val=html_val
                                , rows_val=rows_val
                                , doc_url_hash_val=doc_url_hash_val
                                , element_id_val=element_id_val
                            )

                # Link table with a section
                # Table always has a parent section

                if not parent_id_val == "None":
                    cypher = cypher_pool[7]
                    session.run(cypher
                                    , tb_parent_element_id_val=parent_id_val
                                    , element_id_val=element_id_val
                                )

                else:   # link table to Document
                    cypher = cypher_pool[8]
                    session.run(cypher
                                    , doc_url_hash_val=doc_url_hash_val
                                    , element_id_val=element_id_val
                                )
                countTable += 1
                continue
            # **** for sec in elements: category = 'Table'


        # 5 - Create Image nodes

            if sec.category == 'Image':

                # MERGE Image node

                cypher = cypher_pool[9]
                session.run(cypher, block_idx_val=block_idx_val
                                , page_idx_val=page_idx_val
                                , name_val=text_val
                                , doc_url_hash_val=doc_url_hash_val
                                , element_id_val=element_id_val
                            )

                # Link image with a section
                # Image always linkes to Document

                cypher = cypher_pool[10]
                session.run(cypher
                                , image_parent_element_id_val=doc_url_hash_val
                                , element_id_val=element_id_val
                                , doc_url_hash_val=doc_url_hash_val
                            )

                countImage += 1
                continue
            # **** for sec in elements: category = 'Image'
        # *** for i, sec in enumerate(elements) :

        print(f'\'{doc_url_val}\' Done! Summary: ')
        print('#Sections: ' + str(countSection))
        print('#Chunks: ' + str(countChunk))
        print('#Tables: ' + str(countTable))
        print('#Images: ' + str(countImage))

    # *** with driver.session() as session:

    driver.close()



filename = "IzraelEngleski.pdf"
doc_url = 'IzraelEngleski.pdf'

elements = partition_via_api(
  filename=filename,
  api_url=UNSTRUCTURED_API_URL,
  strategy="hi_res",
#   hi_res_model_name="detectron2_onnx",
  chunking_strategy="by_title",
  max_characters=768,
  multipage_sections=True,
  combine_under_n_chars=100,

)



ingestDocumentNeo4j(elements, doc_url)
