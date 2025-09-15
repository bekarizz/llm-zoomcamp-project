from typing import Dict
from llm import retrieve_cards, build_prompt, call_ollama, extract_sql
from sqlguard import is_safe
from executor import run_sql

def repair_once(question: str, cards, bad_sql: str, error_msg: str) -> str:
    reprompt = build_prompt(question, cards) + \
        f"\nThe previous SQL produced an error:\n{error_msg}\n" \
        f"Please return a corrected SELECT query in a ```sql block."
    resp = call_ollama(reprompt)
    return extract_sql(resp)

def answer(question: str) -> Dict:
    cards = retrieve_cards(question, k=10)
    prompt = build_prompt(question, cards)
    resp = call_ollama(prompt)
    sql = extract_sql(resp)

    if not is_safe(sql):
        return {"error":"unsafe-sql", "sql": sql}

    try:
        df = run_sql(sql)
        return {"sql": sql, "rows": len(df), "preview": df.head(10).to_dict(orient="records")}
    except Exception as e:
        repaired = repair_once(question, cards, sql, str(e))
        if not is_safe(repaired):
            return {"error":"unsafe-sql-repair", "sql": repaired, "orig_sql": sql, "exception": str(e)}
        try:
            df = run_sql(repaired)
            return {"sql": repaired, "orig_sql": sql, "rows": len(df), "preview": df.head(10).to_dict(orient="records"), "repaired": True}
        except Exception as e2:
            return {"error":"repair-failed", "sql": repaired, "orig_sql": sql, "exception": str(e2)}
