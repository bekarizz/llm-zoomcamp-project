# tests/manual_probe.py
from app import answer
for q in [
    "Average bed height on Jig-1 over the last 10 hours?",
    "Tailings chrome by machine in the last 24 hours.",
    "Hourly water flow for Jig-2 yesterday."
]:
    print(q, answer(q))
