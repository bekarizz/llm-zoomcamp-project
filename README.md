# llm-zoomcamp-project
Problem Description

In many industrial environments, large volumes of sensor readings, laboratory quality measurements, and operational logs are stored in relational databases. While this data contains valuable insights, accessing it typically requires knowledge of the database schema, SQL query language, and domain-specific jargon (e.g., knowing that “chrome losses” are stored as tailings_cr2o3_pct).

For plant engineers, analysts, and managers who are not SQL experts, this creates a barrier:

- They must rely on data specialists for even simple questions such as “What was the average bed height on Jig-1 last shift?”

- Writing correct SQL is error-prone because it requires remembering table joins, time functions, and column names.

- As a result, decision-making is delayed, routine monitoring is inefficient, and the value of the industrial data is underutilized.

This project addresses the problem by building a Retrieval-Augmented Generation (RAG) system that allows users to ask questions in natural language and automatically generates the correct SQL query against the database. The system retrieves relevant schema information and examples, guides a large language model to produce safe, validated SQL, executes the query, and returns an answer.


docker compose up -d

pip install sqlalchemy psycopg2-binary pandas numpy qdrant-client sentence-transformers httpx pydantic sqlglot

python gen_data.py

python -m build_index.py

docker exec -it llm-zoomcamp-project-ollama-1 ollama pull llama3.1
