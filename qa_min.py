from sqlalchemy import create_engine, text

SCHEMA_CARD = """
TABLE machines(machine_id,name,area).
TABLE sensors(sensor_id,machine_id,name,unit,description). Join sensors.machine_id -> machines.machine_id.
TABLE telemetry(ts,sensor_id,value). Join telemetry.sensor_id -> sensors.sensor_id.
TABLE lab_samples(sample_id,ts,machine_id,tailings_cr2o3_pct). Join lab_samples.machine_id -> machines.machine_id.
"""

TEMPL = """You write PostgreSQL SELECT only. Use the context to pick tables/joins.
Context:
{schema}
Question: {q}
Return ONLY the SQL in a single line, no explanation.
"""

def llm_generate_sql(question:str)->str:
    # Start super simple: hardcode 2–3 patterns so you get a working demo immediately.
    q=question.lower()
    if "average" in q and "bed" in q and "jig-1" in q:
        return ("SELECT AVG(t.value) AS avg_bed_height_mm "
                "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
                "JOIN machines m ON s.machine_id=m.machine_id "
                "WHERE m.name='Jig-1' AND s.name='bed_height_mm' "
                "AND t.ts >= NOW() - INTERVAL '1000 hours';")
    # Fallback: tiny heuristic
    return ("SELECT date_trunc('hour', t.ts) AS h, AVG(t.value) AS avg_value "
            "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
            "JOIN machines m ON s.machine_id=m.machine_id "
            "WHERE m.name='Jig-1' AND s.name='bed_height_mm' "
            "AND t.ts >= NOW() - INTERVAL '24 hours' GROUP BY 1 ORDER BY 1;")

def run_sql(sql:str):
    eng=create_engine("postgresql+psycopg2://rag:ragpass@localhost:5432/ragdb")
    with eng.begin() as con:
        rows=con.execute(text(sql)).fetchmany(10)
    return rows

if __name__=="__main__":
    q="What was the average bed height on Jig-1 over the last 10 hours?"
    sql=llm_generate_sql(q)
    print("SQL:", sql)
    print("Rows:", run_sql(sql))
