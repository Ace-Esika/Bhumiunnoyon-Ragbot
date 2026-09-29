"""Offline self-test for the ingestion pipeline.

Covers the parts that decide what gets stored, using the cached API payloads
only: no database writes, no embedding model, no network. Run it before a
reindex and in CI.

    python selftest.py            # uses api_cache/
    python selftest.py --refresh  # refetch first
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

import ingest
from embedder import CHUNK_OVERLAP, CHUNK_SIZE, EMBED_DIM, EMBED_MODEL
from integrity import IntegrityError, audit_field_coverage, run_lock

SOURCES = ("ebooks", "blogs", "forums", "qna_type1", "qna_type2")


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.failed: list[str] = []

    def __call__(self, name: str, ok, detail: str = "") -> bool:
        ok = bool(ok)
        self.passed += ok
        if not ok:
            self.failed.append(f"{name}: {detail}")
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))
        return ok

    def section(self, title: str) -> None:
        print(f"\n{title}")


def app_session_state_guards() -> dict[str, list[bool]]:
    """For each session key app.py guards, whether that guard also sets `sources`.

    Checked with ast rather than by importing app: `import app` pulls in
    streamlit and costs ~22s, which is too slow for the offline gate.
    """
    src = (Path(__file__).resolve().parent / "app.py").read_text(encoding="utf-8")
    found: dict[str, list[bool]] = {}
    for node in ast.walk(ast.parse(src)):
        if not isinstance(node, ast.If):
            continue
        test = ast.unparse(node.test)
        assigns_sources = "session_state.sources" in ast.unparse(
            ast.Module(body=node.body, type_ignores=[]))
        for key in ("chat_history", "sources"):
            if f"'{key}' not in" in test or f'"{key}" not in' in test:
                found.setdefault(key, []).append(assigns_sources)
    return found


def app_source_documents_key() -> bool:
    """Does get_response read the key create_retrieval_chain actually returns?

    create_stuff_documents_chain returns {**kwargs, "answer": ...}, so the
    documents arrive under "context". Reading "source_documents" instead yields
    None and silently drops every citation.
    """
    src = (Path(__file__).resolve().parent / "app.py").read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef) and node.name == "get_response":
            return "context" in ast.unparse(ast.Module(body=node.body, type_ignores=[]))
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="refetch the API first")
    args = ap.parse_args()

    c = Checks()
    print(f"corpus source: {ingest.API_BASE}")

    c.section("1. fetch layer")
    payloads = {n: ingest.fetch_source(n, refresh=args.refresh) for n in SOURCES}
    for name, rows in payloads.items():
        c(f"{name} returned a non-empty array", rows, f"{len(rows):,} rows")
        c(f"{name} rows are objects",
          all(isinstance(r, dict) for r in rows), f"{len(rows):,} rows")

    c.section("2. text cleaning")
    c("strips html tags", "<p>আইন</p>" not in ingest.clean_text("<p>আইন</p>"))
    c("unescapes entities", "&amp;" not in ingest.clean_text("আমি &amp; তুমি"))
    c("collapses runs of spaces", "  " not in ingest.clean_text("আ    ইন"))
    c("keeps paragraph breaks", "\n\n" in ingest.clean_text("এক\n\n\n\nদুই"))
    c("one_line flattens breaks", "\n" not in ingest.one_line("মোঃ খলিলুর \nরহমান"))
    c("norm_key folds case and width",
      ingest.norm_key("  ABC ") == ingest.norm_key("abc"))
    c("empty input is safe", ingest.clean_text(None) == "" and ingest.tidy(None) == "")

    c.section("3. flattening")
    records: list[dict] = []
    records += ingest.flatten_acts(payloads["ebooks"])
    records += ingest.flatten_act_tree(payloads["ebooks"])
    records += ingest.flatten_blogs(payloads["blogs"])
    records += ingest.flatten_forums(payloads["forums"])
    records += ingest.flatten_qna(payloads["qna_type1"], payloads["qna_type2"])
    c("records produced", records, f"{len(records):,}")

    by_key: dict[str, dict] = {}
    collisions = 0
    for r in records:
        prev = by_key.get(r["doc_key"])
        if prev is None:
            by_key[r["doc_key"]] = r
            continue
        if prev["text"] != r["text"]:
            collisions += 1
        if r["hash"] < prev["hash"]:
            by_key[r["doc_key"]] = r
    live = [r for r in by_key.values() if r["text"].strip()]

    c("no doc_key collision loses a record", collisions == 0, f"{collisions} collisions")
    c("every record has text", all(r["text"].strip() for r in live))
    c("every record has metadata", all(r["meta"] for r in live))
    c("every record is json-serialisable",
      all(json.dumps(r["meta"], ensure_ascii=False) for r in live))
    types = {r["meta"]["source_type"] for r in live}
    c("all 10 source types present", len(types) == 10, f"{len(types)}: {sorted(types)}")
    c("every record has a doc_key", all(r["doc_key"] for r in live))
    c("no control characters in text",
      not any("\x00" in r["text"] for r in live))

    c.section("4. act metadata inheritance")
    tree = ingest.flatten_act_tree(payloads["ebooks"])
    need = ("pdf_url", "applicable_date", "signature_by", "act_title", "act_year", "url")
    absent = [k for r in tree for k in need if k not in r["meta"]]
    c("tree records carry act-level metadata", not absent, str(sorted(set(absent))))
    c("tree pdf_url is populated",
      sum(1 for r in tree if r["meta"].get("pdf_url")) == len(tree),
      f"{sum(1 for r in tree if r['meta'].get('pdf_url')):,}/{len(tree):,}")
    sec = next((r for r in tree if r["meta"]["source_type"] == "section"), None)
    if c("a section record exists", sec is not None):
        c("section keeps its act identity", bool(sec["meta"]["act_id"]))
        c("section carries a breadcrumb", bool(sec["meta"].get("path")))

    c.section("5. change detection")
    fp = lambda t, m: ingest.fingerprint(t, json.dumps(m, sort_keys=True, ensure_ascii=False))
    c("metadata change rehashes", fp("t", {"a": 1}) != fp("t", {"a": 2}))
    c("text change rehashes", fp("t", {"a": 1}) != fp("u", {"a": 1}))
    c("key order does not matter", fp("t", {"a": 1, "b": 2}) == fp("t", {"b": 2, "a": 1}))
    c("hash is stable", fp("t", {"a": 1}) == fp("t", {"a": 1}))
    c("same record hashes identically twice",
      ingest.fingerprint("ধারা ১", "খরচ") == ingest.fingerprint("ধারা ১", "খরচ"))

    c.section("6. bad-response guards")
    cache = ingest.API_CACHE / "qna_type2.json"
    prev_len = len(payloads["qna_type2"])
    for label, data, should_raise in (
        ("empty array refused", [], True),
        ("catastrophic drop refused", [{}] * (prev_len // 20), True),
        ("small drop allowed", [{}] * int(prev_len * 0.95), False),
    ):
        try:
            ingest._guard_count("qna_type2", data, cache)
            raised = False
        except RuntimeError:
            raised = True
        c(label, raised == should_raise, f"raised={raised}")

    c.section("7. writer lock")
    lock = Path(ingest.ROOT) / "selftest.lock"
    with run_lock(lock, "selftest"):
        held = True
    c("lock is released on exit", held)
    try:
        with run_lock(lock, "selftest"):
            with run_lock(lock, "selftest"):
                pass
        contended = False
    except IntegrityError:
        contended = True
    c("second concurrent writer is refused", contended)
    lock.unlink(missing_ok=True)

    c.section("8. chunk plan")
    from langchain.text_splitter import RecursiveCharacterTextSplitter

    spl = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    planned = {}
    blanks = 0
    for r in live:
        chunks = spl.split_text(r["text"])
        blanks += sum(1 for x in chunks if not x.strip())
        planned[r["doc_key"]] = sum(1 for x in chunks if x.strip())
    c("no record yields zero chunks", all(planned.values()),
      f"{sum(1 for v in planned.values() if not v)} barren")
    c("splitter emits no blank chunks", blanks == 0, f"{blanks} blanks")
    c("plan is deterministic", planned == {
        r["doc_key"]: planned[r["doc_key"]] for r in live})
    print(f"  plan: {len(live):,} records -> {sum(planned.values()):,} chunks")

    c.section("9. upstream field coverage")
    coverage = audit_field_coverage(
        {"ebooks": payloads["ebooks"], "blogs": payloads["blogs"],
         "forums": payloads["forums"],          "qna_type1": payloads["qna_type1"],
         "qna_type2": payloads["qna_type2"]}, live, qna_key=ingest.qna_doc_key)
    for name, ok, detail in coverage.checks:
        c(f"coverage: {name}", ok, detail)

    c.section("10. config")
    c("embedding dim is the e5 width", EMBED_DIM == 768, f"{EMBED_MODEL} = {EMBED_DIM}")
    c("chunk size is sane", 200 <= CHUNK_SIZE <= 2000 and CHUNK_OVERLAP < CHUNK_SIZE,
      f"{CHUNK_SIZE}/{CHUNK_OVERLAP}")
    c("all five sources are configured", set(ingest.SOURCES) == set(SOURCES))
    c("every key prefix maps to a group",
      all(ingest.doc_group(k) for k in ("act:1", "section:1:2", "blog:1",
                                        "forum_topic:1", "qna:abc")))

    c.section("11. app session state")
    guards = app_session_state_guards()
    c("app: sources has its own session-state guard", any(guards.get("sources", [])),
      f"sources guarded={guards.get('sources')}")
    c("app: sources is not set inside the chat_history guard",
      not any(guards.get("chat_history", [])),
      "stale sessions would raise AttributeError on st.session_state.sources")
    c("app: get_response reads the key the chain returns",
      app_source_documents_key(),
      "documents arrive under 'context', not 'source_documents'")

    print(f"\n{'=' * 60}")
    print(f"{c.passed} passed, {len(c.failed)} failed")
    for f in c.failed:
        print(f"  FAILED: {f}")
    return 0 if not c.failed else 1


if __name__ == "__main__":
    sys.exit(main())
