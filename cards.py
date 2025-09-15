from sqlalchemy import create_engine, text
from typing import List, Dict

def make_table_cards() -> List[Dict]:
    return [
        {"type":"table","table":"machines",
         "text":"TABLE machines: plant machines; PK machine_id."},
        {"type":"table","table":"sensors",
         "text":"TABLE sensors: sensors per machine; PK sensor_id; JOIN sensors.machine_id -> machines.machine_id."},
        {"type":"table","table":"telemetry",
         "text":"TABLE telemetry: minute-level values; PK (ts, sensor_id); value is numeric reading; JOIN telemetry.sensor_id -> sensors.sensor_id."},
        {"type":"table","table":"lab_samples",
         "text":"TABLE lab_samples: lab QC (tailings Cr2O3 %); PK sample_id; time ts; JOIN lab_samples.machine_id -> machines.machine_id."},
    ]

SYNONYMS = {
    ("telemetry","value"): ["reading","measurement","signal"],
    ("sensors","name"): ["tag","sensor name"],
    ("sensors","machine_id"): ["machine"],
    ("lab_samples","tailings_cr2o3_pct"): ["chrome in tailings","Cr2O3","losses"],
    ("sensors","bed_height_mm"): ["bed height","bedlevel"],
    ("sensors","pulsation_freq_hz"): ["pulsation","pulse rate","frequency"],
    ("sensors","water_flow_m3h"): ["make-up water","water flow"],
    ("sensors","clayness_index"): ["clay","muddiness","clay index"],
}

def make_column_cards(db_url:str) -> List[Dict]:
    eng = create_engine(db_url)
    out=[]
    with eng.begin() as con:
        tables = ["machines","sensors","telemetry","lab_samples"]
        for t in tables:
            rows = con.execute(text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name=:t ORDER BY ordinal_position
            """), {"t": t}).fetchall()
            for col, dtype in rows:
                syn = SYNONYMS.get((t, col), [])
                s = f"Column {t}.{col} ({dtype}). Synonyms: {', '.join(syn) if syn else '—'}"
                out.append({"type":"column","table":t,"column":col,"text":s})
    return out

def make_example_cards() -> List[Dict]:
    return [
        {"type":"example","text":
         "Q: average bed height for Jig-1 in last 10 hours\n"
         "SQL: SELECT AVG(t.value) AS avg_bed_height_mm "
         "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
         "JOIN machines m ON s.machine_id=m.machine_id "
         "WHERE s.name='bed_height_mm' AND m.name='Jig-1' "
         "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '10 hours';"},
        {"type":"example","text":
         "Q: tailings chrome by machine in last 24 hours\n"
         "SQL: SELECT m.name, AVG(l.tailings_cr2o3_pct) AS avg_cr "
         "FROM lab_samples l JOIN machines m ON l.machine_id=m.machine_id "
         "WHERE l.ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours' "
         "GROUP BY m.name ORDER BY m.name;"},
        {"type":"example","text":
         "Q: hourly make-up water flow for Jig-2 yesterday\n"
         "SQL: SELECT date_trunc('hour', t.ts) AS h, AVG(t.value) AS water_m3h "
         "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
         "JOIN machines m ON s.machine_id=m.machine_id "
         "WHERE s.name='water_flow_m3h' AND m.name='Jig-2' "
         "AND t.ts::date = (CURRENT_DATE - INTERVAL '1 day')::date "
         "GROUP BY 1 ORDER BY 1;"},
    ]
