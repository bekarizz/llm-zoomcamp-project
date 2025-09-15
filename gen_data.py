import numpy as np, pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://rag:ragpass@localhost:5432/ragdb")

with engine.begin() as con:
    con.execute(text(open("schema.sql","r",encoding="utf-8").read()))
    con.execute(text("INSERT INTO machines(name,area) VALUES "
                     "('Jig-1','Plant-A'),('Jig-2','Plant-A');"))
    sensors=[("bed_height_mm","mm","Bed height"),("pulsation_freq_hz","Hz","Frequency"),
             ("water_flow_m3h","m3/h","Water flow"),("clayness_index","0-1","Clay index")]
    rows=[]
    for mid in (1,2):
        for n,u,d in sensors: rows.append({"machine_id":mid,"name":n,"unit":u,"description":d})
    con.execute(text("""INSERT INTO sensors(machine_id,name,unit,description)
                        VALUES (:machine_id,:name,:unit,:description)"""), rows)

start=(datetime.now(timezone.utc)-timedelta(days=3)).replace(second=0,microsecond=0)
end=datetime.now(timezone.utc).replace(second=0,microsecond=0)
idx=pd.date_range(start,end,freq="5min",inclusive="left",tz="UTC")
rng=np.random.default_rng(7)

def wavy(base,amp,period): 
    t=np.arange(len(idx)); return base+amp*np.sin(2*np.pi*t/period)+rng.normal(0,amp*0.1,len(idx))

telemetry=[]
import math
with engine.begin() as con:
    sid_map={ (r.machine_id,r.name):r.sensor_id 
      for r in con.execute(text("SELECT sensor_id,machine_id,name FROM sensors")) }

for mid in (1,2):
    series={
      "bed_height_mm": wavy(120,10,48),
      "pulsation_freq_hz": wavy(2.2,0.2,12),
      "water_flow_m3h": wavy(200,35,36),
      "clayness_index": np.clip(wavy(0.25,0.15,40),0,1)
    }
    for name, vals in series.items():
        sid=sid_map[(mid,name)]
        telemetry += [{"ts":ts.to_pydatetime(),"sensor_id":sid,"value":float(v)} for ts,v in zip(idx,vals)]

with engine.begin() as con:
    for i in range(0,len(telemetry),10000):
        con.execute(text("INSERT INTO telemetry(ts,sensor_id,value) VALUES (:ts,:sensor_id,:value)"),
                    telemetry[i:i+10000])

# Lab samples every 2h, correlated with clay
labs=[]
for mid in (1,2):
    for ts in pd.date_range(start,end,freq="2H",tz="UTC"):
        t=(ts-start).total_seconds()/60
        clay=0.25+0.15*math.sin(2*math.pi*t/200)+rng.normal(0,0.02)
        cr=1.2+1.8*clay+rng.normal(0,0.08)
        labs.append({"ts":ts.to_pydatetime(),"machine_id":mid,"tailings_cr2o3_pct":max(0.1,cr)})

with engine.begin() as con:
    con.execute(text("""INSERT INTO lab_samples(ts,machine_id,tailings_cr2o3_pct)
                        VALUES (:ts,:machine_id,:tailings_cr2o3_pct)"""), labs)
print("Done.")
