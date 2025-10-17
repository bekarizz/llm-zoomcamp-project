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
        {
            "type":"example","text":
            "Q: average bed height for Jig-1 in last 10 hours\n"
            "SQL: SELECT AVG(t.value) AS avg_bed_height_mm "
            "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
            "JOIN machines m ON s.machine_id=m.machine_id "
            "WHERE s.name='bed_height_mm' AND m.name='Jig-1' "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '10 hours';"
         },
        {
            "type":"example","text":
            "Q: tailings chrome by machine in last 24 hours\n"
            "SQL: SELECT m.name, AVG(l.tailings_cr2o3_pct) AS avg_cr "
            "FROM lab_samples l JOIN machines m ON l.machine_id=m.machine_id "
            "WHERE l.ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours' "
            "GROUP BY m.name ORDER BY m.name;"
         },
        {
            "type":"example","text":
            "Q: hourly make-up water flow for Jig-2 yesterday\n"
            "SQL: SELECT date_trunc('hour', t.ts) AS h, AVG(t.value) AS water_m3h "
            "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
            "JOIN machines m ON s.machine_id=m.machine_id "
            "WHERE s.name='water_flow_m3h' AND m.name='Jig-2' "
            "AND t.ts::date = (CURRENT_DATE - INTERVAL '1 day')::date "
            "GROUP BY 1 ORDER BY 1;"
         },
        {
            "type": "example",
            "text":
            "Q: average pulsation frequency for all machines in the last 6 hours\n"
            "SQL: SELECT m.name, AVG(t.value) AS avg_pulsation_hz "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.name = 'pulsation_freq_hz' "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '6 hours' "
            "GROUP BY m.name ORDER BY m.name;"
        },
        {
            "type": "example",
            "text":
            "Q: average water flow for Jig-2 in the last 3 days\n"
            "SQL: SELECT AVG(t.value) AS avg_water_m3h "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.name = 'water_flow_m3h' AND m.name = 'Jig-2' "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '3 days';"
        },
        {
            "type": "example",
            "text":
            "Q: average clayness index for each machine yesterday\n"
            "SQL: SELECT m.name, AVG(t.value) AS avg_clayness "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.name = 'clayness_index' "
            "AND t.ts::date = (CURRENT_DATE - INTERVAL '1 day')::date "
            "GROUP BY m.name ORDER BY m.name;"
        },
        {
            "type": "example",
            "text":
            "Q: correlation between clayness index and tailings chrome for Jig-1 in the last 12 hours\n"
            "SQL: SELECT corr(t.value, l.tailings_cr2o3_pct) AS corr_value "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "JOIN lab_samples l ON l.machine_id = m.machine_id "
            "WHERE s.name = 'clayness_index' AND m.name = 'Jig-1' "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '12 hours' "
            "AND l.ts >= CURRENT_TIMESTAMP - INTERVAL '12 hours';"
        },
        {
            "type": "example",
            "text":
            "Q: maximum bed height for each machine today\n"
            "SQL: SELECT m.name, MAX(t.value) AS max_bed_height_mm "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.name = 'bed_height_mm' "
            "AND t.ts::date = CURRENT_DATE "
            "GROUP BY m.name ORDER BY m.name;"
        },
        {
            "type": "example",
            "text":
            "Q: hourly average of water flow and clayness for Jig-1 over the last 8 hours\n"
            "SQL: SELECT date_trunc('hour', t.ts) AS hour, "
            "AVG(CASE WHEN s.name = 'water_flow_m3h' THEN t.value END) AS avg_water_flow, "
            "AVG(CASE WHEN s.name = 'clayness_index' THEN t.value END) AS avg_clayness "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE m.name = 'Jig-1' "
            "AND s.name IN ('water_flow_m3h','clayness_index') "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '8 hours' "
            "GROUP BY hour ORDER BY hour;"
        },
        {
            "type": "example",
            "text":
            "Q: difference between maximum and minimum bed height for Jig-2 in the last day\n"
            "SQL: SELECT MAX(t.value) - MIN(t.value) AS bed_height_range_mm "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.name = 'bed_height_mm' AND m.name = 'Jig-2' "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours';"
        },
        {
            "type": "example",
            "text":
            "Q: hourly average tailings chrome for each machine today\n"
            "SQL: SELECT m.name, date_trunc('hour', l.ts) AS hour, "
            "AVG(l.tailings_cr2o3_pct) AS avg_cr "
            "FROM lab_samples l "
            "JOIN machines m ON l.machine_id = m.machine_id "
            "WHERE l.ts::date = CURRENT_DATE "
            "GROUP BY m.name, hour ORDER BY m.name, hour;"
        },
        {
            "type": "example",
            "text":
            "Q: number of telemetry records collected per sensor in the last 24 hours\n"
            "SQL: SELECT s.name AS sensor_name, COUNT(*) AS record_count "
            "FROM telemetry t "
            "JOIN sensors s ON t.sensor_id = s.sensor_id "
            "WHERE t.ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours' "
            "GROUP BY s.name ORDER BY record_count DESC;"
        },
        {
            "type": "example",
            "text":
            "Q: average chrome content in tailings per area during the last week\n"
            "SQL: SELECT m.area, AVG(l.tailings_cr2o3_pct) AS avg_cr "
            "FROM lab_samples l "
            "JOIN machines m ON l.machine_id = m.machine_id "
            "WHERE l.ts >= CURRENT_TIMESTAMP - INTERVAL '7 days' "
            "GROUP BY m.area ORDER BY m.area;"
        },
        {
            "type":"example","text":
            "Q: count of machines in the Flotation area\n"
            "SQL: SELECT COUNT(machine_id) "
            "FROM machines "
            "WHERE area = 'Flotation';"
         },
         {
            "type":"example","text":
            "Q: which machine has the sensor with the highest average clayness index over the last week\n"
            "SQL: WITH weekly_avg AS ( "
            "  SELECT s.machine_id, AVG(t.value) AS avg_clayness "
            "  FROM telemetry t JOIN sensors s ON t.sensor_id = s.sensor_id "
            "  WHERE s.name = 'clayness_index' "
            "  AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '7 days' "
            "  GROUP BY 1 "
            ") "
            "SELECT m.name "
            "FROM machines m JOIN weekly_avg wa ON m.machine_id = wa.machine_id "
            "ORDER BY wa.avg_clayness DESC LIMIT 1;"
         },
         {
            "type":"example","text":
            "Q: average water flow when tailings chrome was above 0.5 percent on Jig-4\n"
            "SQL: SELECT AVG(t.value) AS avg_water_flow "
            "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
            "JOIN machines m ON s.machine_id=m.machine_id "
            "WHERE s.name='water_flow_m3h' AND m.name='Jig-4' "
            "AND EXISTS ( "
            "  SELECT 1 FROM lab_samples l "
            "  WHERE l.machine_id = m.machine_id "
            "  AND l.tailings_cr2o3_pct > 0.5 "
            "  AND t.ts::date = l.ts::date "
            ");"
         },
         {
            "type":"example","text":
            "Q: list all sensors that haven't reported a reading in the last hour\n"
            "SQL: SELECT s.name, s.description "
            "FROM sensors s LEFT JOIN telemetry t "
            "ON s.sensor_id = t.sensor_id "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '1 hour' "
            "WHERE t.ts IS NULL;"
         },
         {
            "type":"example","text":
            "Q: average pulsation frequency for Jig-1 compared to Jig-2 over the last week\n"
            "SQL: SELECT "
            "  AVG(CASE WHEN m.name = 'Jig-1' THEN t.value END) AS avg_pulsation_jig1, "
            "  AVG(CASE WHEN m.name = 'Jig-2' THEN t.value END) AS avg_pulsation_jig2 "
            "FROM telemetry t JOIN sensors s ON t.sensor_id=s.sensor_id "
            "JOIN machines m ON s.machine_id=m.machine_id "
            "WHERE s.name='pulsation_freq_hz' "
            "AND m.name IN ('Jig-1', 'Jig-2') "
            "AND t.ts >= CURRENT_TIMESTAMP - INTERVAL '7 days';"
         },
         {"type":"example","text":
            "Q: list plant areas where the average tailings Cr2O3 percentage was above 0.3 this month\n"
            "SQL: SELECT m.area, AVG(l.tailings_cr2o3_pct) AS avg_cr_pct "
            "FROM lab_samples l JOIN machines m ON l.machine_id=m.machine_id "
            "WHERE date_trunc('month', l.ts) = date_trunc('month', CURRENT_TIMESTAMP) "
            "GROUP BY m.area "
            "HAVING AVG(l.tailings_cr2o3_pct) > 0.3 "
            "ORDER BY 2 DESC;"
        },
    ]
