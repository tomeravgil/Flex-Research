from qdrant_client.models import PointStruct


def add_vector(paper_object,vector_id,sql_id,model):
    operation_info = client.upsert(
        collection_name="paper_collection",
        wait=true,
        point=[
            PointStruct(id=vector_id,vector=model.encode(f"{paper_object['title']} [SEP] {paper_object['abstract']}"),payload={
                'sql_id': sql_id,
                'paper_object': paper_object
            })
        ]
    )

