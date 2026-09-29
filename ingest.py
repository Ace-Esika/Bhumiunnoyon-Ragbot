"""Corpus builder for the bhumipedia public API, indexed into PostgreSQL + pgvector.

The corpus is fetched from five public, read-only JSON endpoints rather than
scraped from PDFs, so no OCR is involved:

    GET /api/ebooks/full/       acts with the full sections -> subsections ->
                                schedules -> subschedules tree
    GET /api/blogs/full/        blogs with their HTML body
    GET /api/forums/full/       open forum groups with nested topics
    GET /api/v1/qna/type1/      citizen-service Q&A subset
    GET /api/v1/qna/type2/      the main Q&A table

Each response is cached under api_cache/ so re-runs do not re-download ~40 MB.
The flattened documents are chunked, embedded and upserted into a pgvector
collection, incrementally: a record whose content hash is unchanged is skipped,
a changed record's chunks are purged and rewritten, and a record that vanished
upstream is deleted.

Run:  python ingest.py [--refresh] [--rebuild] [--sources ...] [--index hnsw|ivfflat|none]
"""

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------------------- #
# configuration
# --------------------------------------------------------------------------- #

API_BASE = os.environ.get("API_BASE_URL", "https://bhumipedia.land.gov.bd").rstrip("/")
COLLECTION = os.environ.get("PG_COLLECTION", "land_acts")

ROOT = Path(__file__).resolve().parent
API_CACHE = ROOT / "api_cache"
INDEX_STATE = ROOT / "index_state.json"
RUN_LOCK = ROOT / "ingest.lock"
RUN_REPORT = ROOT / "ingest_report.json"

# name -> (path, doc label). All five are AllowAny and return a plain JSON array.
SOURCES = {
    "ebooks": "/api/ebooks/full/",
    "blogs": "/api/blogs/full/",
    "forums": "/api/forums/full/",
    "qna_type1": "/api/v1/qna/type1/",
    "qna_type2": "/api/v1/qna/type2/",
}

# Reconcile only what was fully fetched. The two Q&A tables are merged into one
# record set, so a run that fetched just one of them would otherwise conclude
# the other's rows had been deleted upstream.
GROUPS = {
    "ebooks": {"ebooks"},
    "blogs": {"blogs"},
    "forums": {"forums"},
    "qna": {"qna_type1", "qna_type2"},
}

# doc_key prefix -> group, mirroring the flatteners' key schemes.
_KEY_GROUP = (
    ("act:", "ebooks"),
    ("act_text:", "ebooks"),
    ("section:", "ebooks"),
    ("subsection:", "ebooks"),
    ("schedule:", "ebooks"),
    ("subschedule:", "ebooks"),
    ("blog:", "blogs"),
    ("forum_group:", "forums"),
    ("forum_topic:", "forums"),
    ("qna:", "qna"),
)


def doc_group(doc_key: str) -> str | None:
    for prefix, group in _KEY_GROUP:
        if doc_key.startswith(prefix):
            return group
    return None

# The embedding model, its dimension and the chunk geometry all come from
# embedder.py so that indexing and querying cannot drift apart.
from embedder import CHUNK_OVERLAP, CHUNK_SIZE, EMBED_DIM, EMBED_MODEL, PrefixedEmbeddings
from integrity import (IntegrityError, Report, audit_field_coverage, run_lock,
                        verify_collection)
from pgurl import database_url

USER_AGENT = "bhumipedia-rag-indexer/2.0"
FETCH_RETRIES = 4
REQUEST_TIMEOUT = 600
INSERT_BATCH = 256
PROGRESS_EVERY = 500

# HNSW settings. Lists this size search fine unindexed, but the index keeps
# latency flat as the corpus grows.
HNSW_M = 16
HNSW_EF_CONSTRUCT = 64

# Bump when the change-detection fingerprint changes shape, so a stored state
# file from an older scheme is rewritten on purpose instead of every record
# quietly mismatching. 2 = text + sorted metadata JSON (1 = text only).
HASH_SCHEME = 2

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

STARTED = time.time()

# Bengali labels for the act tree, so a retrieved chunk states where it came
# from in the same language as the text itself.
KIND_LABEL = {
    "section": "ধারা",
    "subsection": "উপ-ধারা",
    "schedule": "সিডিউল",
    "subschedule": "উপ-সিডিউল",
}


# --------------------------------------------------------------------------- #
# fetch
# --------------------------------------------------------------------------- #

def _fetch(url: str, retries: int = FETCH_RETRIES) -> bytes:
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
                return r.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            last = exc
            wait = 2 ** attempt
            print(f"    retry {attempt + 1}/{retries} after {wait}s ({type(exc).__name__}: {exc})")
            time.sleep(wait)
    raise RuntimeError(f"failed to fetch {url}: {last}")


# All five endpoints hold hundreds to tens of thousands of records, so an empty
# or sharply shrunken array is a bad response rather than a mass deletion
# upstream. build() would read it as "these records are gone" and purge the
# group, so refuse it before the response reaches the cache or the index.
MIN_RATIO_TO_CACHED = 0.5


def _guard_count(name: str, data: list, cache: Path) -> None:
    if not data:
        raise RuntimeError(
            f"{name}: the API returned an empty array. Reconciling that would "
            f"purge every indexed record of this source, so nothing was written. "
            f"Check {SOURCES[name]} and re-run when it is healthy."
        )
    if not cache.exists():
        return
    try:
        prev = json.loads(cache.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(prev, list) or not prev:
        return
    if len(data) < len(prev) * MIN_RATIO_TO_CACHED:
        raise RuntimeError(
            f"{name}: {len(data):,} records but the cache holds {len(prev):,}. A "
            f"drop this large is far more likely a truncated response than a real "
            f"deletion, and reconciling it would purge the group. Inspect "
            f"{cache.name}, then re-run."
        )


def _authoritative_count(name: str) -> int | None:
    """Row total the API itself reports, for cross-checking the full array.

    The Q&A endpoints switch to a paginated envelope when `page` is present, and
    that envelope carries `count`. Comparing it with the length of the unpaginated
    array is the only way to notice that a "full" fetch was silently truncated.
    """
    if not name.startswith("qna"):
        return None
    url = f"{API_BASE}{SOURCES[name]}?page=1&page_size=1"
    try:
        payload = json.loads(_fetch(url, retries=2).decode("utf-8"))
    except (RuntimeError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict) and isinstance(payload.get("count"), int):
        return payload["count"]
    return None


def _check_complete(name: str, data: list) -> None:
    """Refuse a payload that is smaller than the API's own row count."""
    claimed = _authoritative_count(name)
    if claimed is None:
        return
    if len(data) != claimed:
        raise RuntimeError(
            f"{name}: fetched {len(data):,} rows but the API reports count="
            f"{claimed:,}. A short payload means records would be treated as "
            f"deleted upstream and purged from the index. Re-run; if it persists "
            f"the endpoint is serving a truncated response."
        )
    if name in ("qna_type1", "qna_type2"):
        ids = [r.get("id") for r in data if isinstance(r.get("id"), int)]
        if ids and max(ids) != claimed:
            raise RuntimeError(
                f"{name}: highest row id is {max(ids):,} but the API reports "
                f"count={claimed:,}; ids are 1-based positions, so rows are missing."
            )


def fetch_source(name: str, refresh: bool = False) -> list[dict]:
    """Fetch one endpoint, reusing the on-disk cache unless --refresh is given."""
    path = SOURCES[name]
    cache = API_CACHE / f"{name}.json"
    if cache.exists() and not refresh:
        data = json.loads(cache.read_text(encoding="utf-8"))
        print(f"  {name}: {len(data)} records (cached)")
        return data

    url = f"{API_BASE}{path}"
    print(f"  {name}: GET {url}")
    data = json.loads(_fetch(url).decode("utf-8"))
    if not isinstance(data, list):
        raise RuntimeError(f"{name}: expected a JSON array, got {type(data).__name__}")
    _check_complete(name, data)
    _guard_count(name, data, cache)

    API_CACHE.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"  {name}: {len(data)} records "
          f"({cache.stat().st_size / 1e6:.1f} MB cached)")
    return data


# --------------------------------------------------------------------------- #
# text helpers
# --------------------------------------------------------------------------- #

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t\r\f\v]+")
_NL = re.compile(r"\n{3,}")


def clean_text(value) -> str:
    """Flatten HTML, unescape entities and normalise whitespace."""
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"(?i)<(br|/p|/div|/li|/h[1-6])\s*/?>", "\n", text)
    text = _TAG.sub(" ", text)
    text = html.unescape(text)
    text = _WS.sub(" ", text)
    text = _NL.sub("\n\n", text)
    return "\n".join(line.strip() for line in text.splitlines()).strip()


def norm_key(value) -> str:
    """Fold a string for identity comparison: NFKC, collapsed space, casefold."""
    return _WS.sub(" ", unicodedata.normalize("NFKC", str(value or ""))).strip().casefold()


def tidy(value) -> str:
    """Trim a stored value without changing its internal content.

    The live data carries warts such as the category 'dag ' with a trailing
    space; keeping them would fragment metadata filters.
    """
    return str(value or "").strip()


_ALL_WS = re.compile(r"\s+")


def one_line(value) -> str:
    """Flatten to a single line.

    clean_text deliberately keeps paragraph breaks, but fields that get spliced
    into a ' | ' header or a metadata value are wrapped upstream across lines
    (a signature name reads 'মোঃ খলিলুর \\nরহমান'), so they must not keep a break.
    """
    return _ALL_WS.sub(" ", clean_text(value)).strip()


def fingerprint(*parts) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(norm_key(p).encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def labelled(kind: str, number, heading) -> str:
    """Render one node of the act tree as 'ধারা ৫. খরচ'."""
    bits = []
    if number not in (None, ""):
        bits.append(f"{KIND_LABEL[kind]} {tidy(number)}")
    head = tidy(heading)
    if head:
        bits.append(head)
    return ". ".join(bits)


# --------------------------------------------------------------------------- #
# flattening
# --------------------------------------------------------------------------- #
# Each record is a dict of {doc_key, text, meta}. doc_key is the stable identity
# used for incremental updates, so it must derive only from upstream ids.

def _act_url(act_id) -> str:
    return f"{API_BASE}/acts/{act_id}/"


def _act_facts(act: dict) -> str:
    # Several of these carry embedded newlines upstream (a signature name is
    # wrapped across lines), so they go through clean_text to stay on one line.
    applicable = one_line(act.get("applicable_date_bn")) or one_line(act.get("applicable_date_en"))
    # Both scripts are kept: half the acts carry only one language, and a query
    # may name either, so the English date is not redundant with the Bengali.
    applicable_en = one_line(act.get("applicable_date_en"))
    if applicable_en and applicable_en != applicable:
        applicable = f"{applicable} ({applicable_en})" if applicable else applicable_en
    signer = ", ".join(x for x in (
        one_line(act.get("signature_by")),
        one_line(act.get("signature_position")),
    ) if x)
    return " | ".join(
        f for f in (
            f"প্রকার: {tidy(act.get('ebooks_type'))}" if act.get("ebooks_type") else "",
            f"নম্বর: {tidy(act.get('number'))}" if act.get("number") else "",
            f"বছর: {tidy(act.get('act_year'))}" if act.get("act_year") else "",
            f"প্রকাশ: {tidy(act.get('publication_date'))}" if act.get("publication_date") else "",
            f"প্রকাশক: {tidy(act.get('publication_by'))}" if act.get("publication_by") else "",
            f"প্রযোজ্য: {applicable}" if applicable else "",
            f"স্বাক্ষরকারী: {signer}" if signer else "",
            f"শাখা: {tidy(act.get('branch'))}" if act.get("branch") else "",
        ) if f
    )



def _alt_pdf(act: dict) -> str:
    """The other of the two PDF locations, when an act carries both.

    `file` is the uploaded original and `system_generated_pdf` the rendered
    one. Keeping the second means neither upstream value is dropped, which
    matters when the original is a scan and the generated copy is not.
    """
    a = tidy(act.get("file"))
    b = tidy(act.get("system_generated_pdf"))
    primary = a or b
    return b if b and b != primary else (a if a and a != primary else "")


def _act_meta(act: dict) -> dict:
    act_id = act.get("id")
    return {
        "act_id": act_id,
        "act_title": tidy(act.get("title_of_act")),
        "act_year": tidy(act.get("act_year")),
        "ebooks_type": tidy(act.get("ebooks_type")),
        "act_number": tidy(act.get("number")),
        "number": tidy(act.get("number")),
        "publication_date": tidy(act.get("publication_date")),
        "publication_by": tidy(act.get("publication_by")),
        "applicable_date": one_line(act.get("applicable_date_bn"))
                           or one_line(act.get("applicable_date_en")),
        "applicable_date_bn": one_line(act.get("applicable_date_bn")),
        "applicable_date_en": one_line(act.get("applicable_date_en")),
        "signature_by": one_line(act.get("signature_by")),
        "signature_position": one_line(act.get("signature_position")),
        # The branch is the issuing office, which is what a citation needs when
        # two acts share a year and number.
        "branch": tidy(act.get("branch")),
        "sub_branch": tidy(act.get("sub_branch")),
        # The API hands over the act's real PDF location. The site route below
        # is an SPA fallback, so prefer these when citing a source.
        "pdf_url": tidy(act.get("file")) or tidy(act.get("system_generated_pdf")),
        "pdf_url_alt": _alt_pdf(act),
        "url": _act_url(act_id),
    }


def flatten_acts(acts: list[dict]) -> list[dict]:
    """Emit two records per act: an identity header and its free-text body.

    The act's own `schedules` field is a free-text blob, not the nested
    schedule objects, and it shares no lines with the section tree. For the 15
    largest acts it *is* the entire body - act 379 (ভূমি প্রশাসন ম্যানুয়াল ১ম খন্ড)
    has no sections at all and 1.16M characters here - so it is indexed as a
    separate chunked record rather than folded into the header.
    """
    records = []
    for act in acts:
        act_id = act.get("id")
        title = tidy(act.get("title_of_act"))
        facts = _act_facts(act)
        head = "\n".join(x for x in (title, facts) if x)

        meta = _act_meta(act)
        meta["source_type"] = "act"
        meta["heading"] = tidy(act.get("heading"))
        records.append({"doc_key": f"act:{act_id}", "text": head, "meta": meta})

        body = "\n".join(
            x for x in (
                clean_text(act.get("heading")),
                clean_text(act.get("motto")),
                clean_text(act.get("objective")),
                clean_text(act.get("proposal")),
                clean_text(act.get("schedules")),
                clean_text(act.get("copy_to")),
            ) if x
        )
        footer = clean_text(act.get("footer"))
        if body or footer:
            bmeta = _act_meta(act)
            bmeta["source_type"] = "act_text"
            records.append({
                "doc_key": f"act_text:{act_id}",
                "text": "\n\n".join(x for x in (head, body, footer) if x),
                "meta": bmeta,
            })
    return records



def _tree_record(kind, act, crumb, node, path) -> dict:
    """One section/subsection/schedule/subschedule of an act."""
    title = labelled(kind, node.get("number"), node.get("heading"))
    body = clean_text(node.get("content"))
    note = clean_text(node.get("note"))
    text = "\n".join(x for x in (title, body, f"নোট: {note}" if note else "") if x)
    # Inherit the act-level facts (type, number, year, effective date,
    # signatory, real PDF) so a section chunk is citable on its own - a section
    # is what a question about a specific ধারা actually retrieves. `number` is
    # overridden below because a node's number is the section number, not the
    # act's.
    meta = {
        **_act_meta(act),
        "source_type": kind,
        "path": crumb,
        "heading": tidy(node.get("heading")),
        "number": tidy(node.get("number")),
    }
    for key in ("section", "subsection", "schedule", "subschedule"):
        if key in path:
            meta[f"{key}_id"] = path[key].get("id")
            meta[f"{key}_number"] = tidy(path[key].get("number"))
    if kind in ("section", "subsection"):
        meta["node_id"] = node.get("id")
    return {"doc_key": f"{kind}:" + ":".join(str(path[k].get("id")) for k in path), "text": text, "meta": meta}


def flatten_act_tree(acts: list[dict]) -> list[dict]:
    """The sections -> subsections -> schedules -> subschedules tree."""
    records = []
    for act in acts:
        atitle = tidy(act.get("title_of_act"))
        for sec in act.get("sections") or []:
            p1 = {"section": sec}
            c1 = atitle
            records.append(_tree_record("section", act, c1, sec, p1))
            for sub in sec.get("subsections") or []:
                p2 = {**p1, "subsection": sub}
                c2 = f"{atitle} > {labelled('section', sec.get('number'), sec.get('heading'))}"
                records.append(_tree_record("subsection", act, c2, sub, p2))
                for sch in sub.get("schedules") or []:
                    p3 = {**p2, "schedule": sch}
                    c3 = f"{c2} > {labelled('subsection', sub.get('number'), sub.get('heading'))}"
                    records.append(_tree_record("schedule", act, c3, sch, p3))
                    for ss in sch.get("subschedules") or []:
                        p4 = {**p3, "subschedule": ss}
                        c4 = f"{c3} > {labelled('schedule', sch.get('number'), sch.get('heading'))}"
                        records.append(_tree_record("subschedule", act, c4, ss, p4))
            for sch in sec.get("schedules") or []:
                p2 = {**p1, "schedule": sch}
                c2 = f"{atitle} > {labelled('section', sec.get('number'), sec.get('heading'))}"
                records.append(_tree_record("schedule", act, c2, sch, p2))
                for ss in sch.get("subschedules") or []:
                    p3 = {**p2, "subschedule": ss}
                    c3 = f"{c2} > {labelled('schedule', sch.get('number'), sch.get('heading'))}"
                    records.append(_tree_record("subschedule", act, c3, ss, p3))
    return records


def flatten_blogs(blogs: list[dict]) -> list[dict]:
    records = []
    for b in blogs:
        title = tidy(b.get("title_name"))
        body = clean_text(b.get("content"))
        head = "\n".join(x for x in (title, tidy(b.get("author"))) if x)
        records.append({
            "doc_key": f"blog:{b.get('id')}",
            "text": f"{head}\n\n{body}".strip(),
            "meta": {
                "source_type": "blog",
                "blog_id": b.get("id"),
                "title": title,
                "author": tidy(b.get("author")),
                "created_date": tidy(b.get("created_date")),
                "url": f"{API_BASE}/blogs/{b.get('id')}/",
            },
        })
    return records


def flatten_forums(groups: list[dict]) -> list[dict]:
    records = []
    for g in groups:
        name = tidy(g.get("name"))
        gid = g.get("id")
        records.append({
            "doc_key": f"forum_group:{gid}",
            "text": f"{name}\n\n{clean_text(g.get('description'))}".strip(),
            "meta": {
                "source_type": "forum_group",
                "group_id": gid,
                "title": name,
                "badge": tidy(g.get("badge")),
                "group_type": tidy(g.get("group_type")),
                "topic_count": g.get("topic_count"),
                "created_date": tidy(g.get("created_date")),
                "url": f"{API_BASE}/forums/{gid}/",
            },
        })
        for t in g.get("topics") or []:
            title = tidy(t.get("title"))
            records.append({
                "doc_key": f"forum_topic:{t.get('id')}",
                "text": f"{name} > {title}\n\n{clean_text(t.get('description'))}".strip(),
                "meta": {
                    "source_type": "forum_topic",
                    "group_id": gid,
                    "group_name": name,
                    "topic_id": t.get("id"),
                    "title": title,
                    "status": tidy(t.get("status")),
                    "is_pinned": bool(t.get("is_pinned")),
                    "created_date": tidy(t.get("created_date")),
                    "url": f"{API_BASE}/forums/{gid}/topics/{t.get('id')}/",
                },
            })
    return records


def flatten_qna(type1: list[dict], type2: list[dict]) -> list[dict]:
    """Merge the two Q&A tables, collapsing repeated rows.

    The two live tables overlap only partly (1,065 and 24,995 rows, 686 shared
    answers) and type2 repeats a single question/answer pair up to 108 times,
    so exact rows are collapsed on (question, answer) and the surviving record
    keeps the set of tables and ids it came from.
    """
    merged: dict[tuple[str, str], dict] = {}
    for tag, rows in (("type1", type1), ("type2", type2)):
        for r in rows:
            q = norm_key(r.get("question"))
            a = norm_key(r.get("answer"))
            if not q and not a:
                continue
            rec = merged.setdefault((q, a), {
                "question": clean_text(r.get("question")),
                "answer": clean_text(r.get("answer")),
                "category": tidy(r.get("category")),
                "categories": set(),
                "keywords": set(),
                "tags": set(),
                "ids": set(),
            })
            rec["tags"].add(tag)
            rec["ids"].add(r.get("id"))
            if tidy(r.get("keyword")):
                rec["keywords"].add(tidy(r.get("keyword")))
            if tidy(r.get("category")):
                rec["categories"].add(tidy(r.get("category")))
             # Prefer the populated label when the two tables disagree.
            if not rec["category"] and tidy(r.get("category")):
                rec["category"] = tidy(r.get("category"))

    records = []
    for rec in merged.values():
        q, a = rec["question"], rec["answer"]
        if not a:
            continue
        kw = ", ".join(sorted(rec["keywords"]))
        head = "\n".join(x for x in (f"প্রশ্ন: {q}", f"উত্তর: {a}") if x)
        # The category slug (khotian, namjari, khajna, ...) was only metadata,
        # so a query naming a category could not reach the answer. The two
        # tables disagree on the label for the same answer, so every distinct
        # label is kept and searchable rather than one of them being dropped.
        cats = ", ".join(sorted(rec["categories"]))
        cat = rec["category"] or cats
        tail = [x for x in (f"বিভাগ: {cats}" if cats else "",
                            f"সংশ্লিষ্ট শব্দ: {kw}" if kw else "") if x]
        body = head + ("\n\n" + "\n".join(tail) if tail else "")
        records.append({
            "doc_key": f"qna:{fingerprint(q, a)}",
            "text": body,
            "meta": {
                "source_type": "qna",
                "qna_tables": ",".join(sorted(rec["tags"])),
                "in_type1": "type1" in rec["tags"],
                "category": cat,
                "categories": cats,
                "keyword": kw[:500],
                "question": q[:500],
                "qna_ids": ",".join(str(i) for i in sorted(x for x in rec["ids"] if x is not None))[:200],
                "url": f"{API_BASE}/qna/",
            },
        })
    return records


# --------------------------------------------------------------------------- #
# store
# --------------------------------------------------------------------------- #

def get_embeddings():
    return PrefixedEmbeddings(EMBED_MODEL)


def _create_engine(url: str):
    from sqlalchemy import create_engine

    return create_engine(url)


def _stored_vector_dim(engine) -> int | None:
    """Width of the existing embedding column, or None if the table is absent."""
    with engine.connect() as conn:
        return conn.exec_driver_sql(
            "SELECT (regexp_match(format_type(atttypid, atttypmod), 'vector\\((\\d+)\\)'))[1]::int "
            "FROM pg_attribute "
            "WHERE attrelid = 'langchain_pg_embedding'::regclass AND attname = 'embedding'"
        ).scalar()


def connect_store(embeddings, rebuild: bool = False):
    from langchain_postgres import PGVector

    if not database_url():
        raise SystemExit("DATABASE_URL is not set. Add it to .env, e.g.\n"
                         "  DATABASE_URL=postgresql+psycopg://rag:rag@127.0.0.1:55432/rag")

    engine = _create_engine(database_url())
    try:
        existing = _stored_vector_dim(engine)
    except Exception:
        existing = None  # table does not exist yet; PGVector will create it
    if existing is not None and existing != EMBED_DIM:
        # PGVector only creates the table when absent, so a change of embedding
        # dimension would otherwise fail against the stale vector(N) column.
        print(f"  stored column is vector({existing}) but {EMBED_MODEL} is "
              f"{EMBED_DIM}-dim - dropping the table")
        with engine.begin() as conn:
            conn.exec_driver_sql("DROP TABLE IF EXISTS langchain_pg_embedding CASCADE")
        state_path = INDEX_STATE
        if state_path.exists():
            state_path.unlink()

    return PGVector(
        embeddings=embeddings,
        connection=database_url(),
        embedding_length=EMBED_DIM,
        collection_name=COLLECTION,
        distance_strategy="cosine",
        pre_delete_collection=rebuild,
        use_jsonb=True,
        create_extension=True,
    )


def store_counts(store) -> tuple[int, int]:
    with store._engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT (SELECT count(*) FROM langchain_pg_embedding e "
            "        JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "        WHERE c.name = %s), "
            "       (SELECT count(DISTINCT e.cmetadata->>'doc_key') FROM langchain_pg_embedding e "
            "        JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "        WHERE c.name = %s)",
            (COLLECTION, COLLECTION),
        ).one()
    return int(rows[0]), int(rows[1])


def purge_doc_keys(store, doc_keys) -> int:
    """Delete every chunk of the given records with one statement.

    PGVector.delete() only accepts ids, and re-reading each row just to discard
    it costs a round trip per chunk, so a record-level purge goes through SQL.
    """
    keys = [k for k in doc_keys if k]
    if not keys:
        return 0
    with store._engine.begin() as conn:
        res = conn.exec_driver_sql(
            "DELETE FROM langchain_pg_embedding e USING langchain_pg_collection c "
            "WHERE e.collection_id = c.uuid AND c.name = %s "
            "  AND e.cmetadata->>'doc_key' = ANY(%s)",
            (COLLECTION, keys),
        )
    return res.rowcount or 0


def ensure_ann_index(store, kind: str) -> None:
    if kind == "none":
        return
    emb = "langchain_pg_embedding"
    if kind == "hnsw":
        ddl = (f"CREATE INDEX IF NOT EXISTS {emb}_hnsw_idx ON {emb} "
               f"USING hnsw (embedding vector_cosine_ops) "
               f"WITH (m = {HNSW_M}, ef_construction = {HNSW_EF_CONSTRUCT})")
    else:
        ddl = (f"CREATE INDEX IF NOT EXISTS {emb}_ivfflat_idx ON {emb} "
               f"USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)")
    print(f"  building {kind} index (one-off, minutes on a large corpus)")
    with store._engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    with store._engine.begin() as conn:
        conn.exec_driver_sql("ANALYZE langchain_pg_embedding")
    print(f"  {kind} index ready")


# --------------------------------------------------------------------------- #
# index
# --------------------------------------------------------------------------- #

def ensure_meta_indexes(store) -> None:
    """Indexes for the metadata filters retrieval actually uses.

    Vector search is the slow path, but filtering by source type, act or Q&A
    category is a plain equality test on an expression over jsonb; without these
    it degrades into a sequential scan of every chunk.
    """
    emb = "langchain_pg_embedding"
    for name, expr in (("src", "cmetadata->>'source_type'"),
                       ("act", "cmetadata->>'act_id'"),
                       ("cat", "cmetadata->>'category'"),
                       ("chunk", "cmetadata->>'doc_key'")):
        ddl = (f"CREATE INDEX IF NOT EXISTS {emb}_{name}_idx ON {emb} "
               f"(collection_id, ({expr}))")
        with store._engine.begin() as conn:
            conn.exec_driver_sql(ddl)
    print("  metadata indexes ready")


def load_state() -> dict:
    if INDEX_STATE.exists():
        try:
            return json.loads(INDEX_STATE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            print("  ! state file unreadable, rebuilding from scratch")
    return {}


def save_state(state: dict, records: int, chunks: int) -> None:
    INDEX_STATE.write_text(
        json.dumps({
            "backend": "pgvector",
            "collection": COLLECTION,
            "api_base": API_BASE,
            "sources": sorted(SOURCES),
            "embed_model": EMBED_MODEL,
            "embedding_dim": EMBED_DIM,
            "chunk": [CHUNK_SIZE, CHUNK_OVERLAP],
            "hash_scheme": HASH_SCHEME,
            "indexed": state.get("indexed", {}),
            "records": records,
            "chunks": chunks,
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def save_run_report(report: Report, *, records: int, chunks: int, written: int,
                    sources: dict, elapsed: float, ok: bool) -> None:
    """Leave a machine-readable trace of the run next to the state file."""
    RUN_REPORT.write_text(json.dumps({
        "ok": ok,
        "collection": COLLECTION,
        "embed_model": EMBED_MODEL,
        "hash_scheme": HASH_SCHEME,
        "records": records,
        "chunks": chunks,
        "written": written,
        "elapsed_min": round(elapsed, 2),
        "source_records": sources,
        "checks": [{"name": n, "ok": ok_, "detail": d} for n, ok_, d in report.checks],
        "stats": report.stats,
        "problems": report.problems,
    }, indent=2, ensure_ascii=False), encoding="utf-8")


def build(payloads: dict[str, list[dict]], rebuild: bool = False, index: str = "hnsw") -> None:
    from langchain.text_splitter import RecursiveCharacterTextSplitter

    records: list[dict] = []
    if "ebooks" in payloads:
        records += flatten_acts(payloads["ebooks"])
        records += flatten_act_tree(payloads["ebooks"])
    if "blogs" in payloads:
        records += flatten_blogs(payloads["blogs"])
    if "forums" in payloads:
        records += flatten_forums(payloads["forums"])
    if "qna_type1" in payloads or "qna_type2" in payloads:
        records += flatten_qna(payloads.get("qna_type1", []), payloads.get("qna_type2", []))

    # One identity per (doc_key, text, metadata). A stored record is chunk text
    # plus cmetadata, so identity has to cover both: hashing the text alone made
    # a metadata-only correction (a fixed pdf_url, a renamed field) keep its
    # hash and never be rewritten, silently leaving the old values indexed.
    # Metadata is not part of doc_key, so re-embedding still purges the previous
    # chunks of that record rather than duplicating them.
    for r in records:
        r["hash"] = fingerprint(r["text"], json.dumps(r["meta"], sort_keys=True,
                                                    ensure_ascii=False))
    by_key: dict[str, dict] = {}
    collisions: list[str] = []
    for r in records:
        prev = by_key.get(r["doc_key"])
        if prev is None:
            by_key[r["doc_key"]] = r
            continue
        # doc_key comes from the upstream id, so two different bodies under one
        # key means one of them is being discarded. That is silent data loss and
        # has to be visible, not resolved by picking the lower hash.
        if prev["text"] != r["text"]:
            collisions.append(r["doc_key"])
        if r["hash"] < prev["hash"]:
            by_key[r["doc_key"]] = r
    records = list(by_key.values())
    if collisions:
        print(f"  ! {len(collisions)} doc_key collisions, one copy kept per key: "
              f"{collisions[:5]}")

    dropped = [r for r in records if not r["text"].strip()]
    records = [r for r in records if r["text"].strip()]
    print(f"\nflattened {len(records)} records ({len(dropped)} empty and skipped)")
    if dropped:
        print(f"  empty doc_keys: {[r['doc_key'] for r in dropped][:5]}")

    # Only groups whose sources were all fetched this run may be reconciled.
    # Records outside them are neither purged nor rewritten, otherwise a partial
    # run would rewrite the merged Q&A with one table's half of the content.
    fetched = set(payloads)
    reconcile = {g for g, srcs in GROUPS.items() if srcs <= fetched}
    partial = {g: sorted(srcs - fetched) for g, srcs in GROUPS.items()
               if (srcs & fetched) and srcs - fetched}
    for g in sorted(partial):
        print(f"  not reconciling {g}: also needs {', '.join(partial[g])} "
              f"to be fetched together")
    print(f"  reconciling: {', '.join(sorted(reconcile)) or '(nothing to do)'}")

    if not reconcile:
        print("\nnothing to reconcile; collection left untouched")
        return

    skipped = [r for r in records if doc_group(r["doc_key"]) not in reconcile]
    if skipped:
        print(f"  {len(skipped):,} records skipped: sources not fully fetched")
        records = [r for r in records if doc_group(r["doc_key"]) in reconcile]
        dropped = []

    kinds: dict[str, int] = {}
    for r in records:
        kinds[r["meta"]["source_type"]] = kinds.get(r["meta"]["source_type"], 0) + 1
    for k, n in sorted(kinds.items(), key=lambda x: -x[1]):
        print(f"  {k:14s} {n:>6,}")

    state = load_state()
    stale = (
        rebuild
        or state.get("embed_model") != EMBED_MODEL
        or state.get("embedding_dim") != EMBED_DIM
        or state.get("chunk") != [CHUNK_SIZE, CHUNK_OVERLAP]
        or state.get("hash_scheme") != HASH_SCHEME
    )

    if stale and not rebuild:
        print("  embedding, chunking or fingerprint config changed - full rewrite")

    # A full rewrite must drop the collection, not just re-embed over it: chunk
    # ids are "<doc_key>#<index>", so a record that now yields fewer chunks than
    # it used to would leave its trailing old chunks behind as orphans that no
    # purge ever reaches.
    if stale:
        indexed = {}
        gone = []
        todo = records
        changed = []
    else:
        indexed = dict(state.get("indexed", {}))
        live = {r["doc_key"] for r in records}
        # A record belongs to the group of its doc_key prefix, so keys held in
        # the state file for groups outside this run are left strictly alone.
        missing = set(indexed) - live
        gone = sorted(k for k in missing if doc_group(k) in reconcile)

        todo = [r for r in records if indexed.get(r["doc_key"]) != r["hash"]]
        changed = [r for r in todo if r["doc_key"] in indexed]

        print(f"  {len(records) - len(todo)} unchanged, {len(changed)} changed, "
              f"{len(todo) - len(changed)} new, {len(gone)} removed upstream")

    embeddings = get_embeddings()
    store = connect_store(embeddings, rebuild=stale)

    if stale:
        print(f"  {'--rebuild' if rebuild else 'config change'}: collection dropped and recreated")

    if gone or changed:
        keys = gone + [r["doc_key"] for r in changed]
        n = purge_doc_keys(store, keys)
        print(f"  purged {n} chunks of {len(keys)} records")
        if n == 0 and changed:
            # Keeping the old chunks would corrupt the index, so say so loudly
            # rather than letting the insert quietly upsert over the top.
            raise RuntimeError(
                f"purge matched no rows for {len(changed)} changed records, so stale "
                f"chunks would survive. Check that cmetadata->>'doc_key' is populated "
                f"and matches the stored keys, or re-run with --rebuild."
            )
        # A record purged upstream must also leave the state file, otherwise it
        # still looks indexed and would never be re-embedded.
        for key in gone:
            indexed.pop(key, None)

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)

    # The plan is computed for every in-scope record, not just the ones being
    # rewritten, because it is the expectation the database is checked against
    # at the end. Splitting is cheap next to embedding.
    planned: dict[str, int] = {}
    chunk_cache: dict[str, list[str]] = {}
    for r in records:
        chunks = [c for c in splitter.split_text(r["text"]) if c.strip()]
        chunk_cache[r["doc_key"]] = chunks
        planned[r["doc_key"]] = len(chunks)

    barren = [k for k, n in planned.items() if n == 0]
    if barren:
        raise IntegrityError(
            f"{len(barren)} records produced no chunks at all, so they would be "
            f"silently absent from the index: {barren[:5]}"
        )

    done = 0
    written = 0
    batch_texts: list[str] = []
    batch_meta: list[dict] = []
    batch_ids: list[str] = []

    def flush():
        nonlocal written
        if batch_texts:
            store.add_texts(batch_texts, metadatas=batch_meta, ids=batch_ids)
            written += len(batch_texts)
            batch_texts.clear()
            batch_meta.clear()
            batch_ids.clear()

    if todo:
        print(f"  embedding {len(todo)} records")
        for i, r in enumerate(todo, 1):
            chunks = chunk_cache[r["doc_key"]]
            for ci, chunk in enumerate(chunks):
                meta = dict(r["meta"])
                # The record identity has to be in the stored metadata: the
                # purge and the distinct-record count both read it back with
                # cmetadata->>'doc_key'.
                meta["doc_key"] = r["doc_key"]
                meta["chunk_index"] = ci
                meta["chunk_total"] = len(chunks)
                batch_texts.append(chunk)
                batch_meta.append(meta)
                batch_ids.append(f"{r['doc_key']}#{ci}")
            indexed[r["doc_key"]] = r["hash"]
            if len(batch_texts) >= INSERT_BATCH:
                flush()
            done += 1
            if done % PROGRESS_EVERY == 0:
                print(f"    {done}/{len(todo)} records, {written:,} chunks", flush=True)
        flush()
    else:
        print("  index already up to date")

    ensure_ann_index(store, index)
    ensure_meta_indexes(store)
    total, distinct = store_counts(store)

    # Success is only claimed once the database agrees with the plan.
    scope = tuple(p for p, _ in _KEY_GROUP)
    report = verify_collection(
        store._engine, COLLECTION, planned=planned, scope_prefixes=scope,
        dim=EMBED_DIM, expect_ann=index)
    report.add("no doc_key collision discarded a record", not collisions,
               f"{len(collisions)} collided keys" + (f": {collisions[:5]}" if collisions else ""))
    report.add("no record dropped for empty text", not dropped,
               f"{len(dropped)} empty" + (f": {[r['doc_key'] for r in dropped][:5]}" if dropped else ""))
    print("\n" + report.render("index integrity"))
    if not report.ok:
        save_run_report(report, records=len(records), chunks=total, written=written,
                        sources={k: len(v) for k, v in payloads.items()},
                        elapsed=time.time() - STARTED, ok=False)
        report.raise_if_failed("index integrity")

    save_state({**state, "indexed": indexed}, records=len(records), chunks=total)
    save_run_report(report, records=len(records), chunks=total, written=written,
                    sources={k: len(v) for k, v in payloads.items()},
                    elapsed=time.time() - STARTED, ok=True)
    print(f"  {written:,} chunks written; collection now holds "
          f"{total:,} chunks across {distinct:,} records")


# --------------------------------------------------------------------------- #

def qna_doc_key(row: dict) -> str:
    """The doc_key a Q&A payload row flattens to, for the coverage audit."""
    return "qna:" + fingerprint(clean_text(row.get("question")),
                                clean_text(row.get("answer")))


def verify_only(payloads: dict[str, list[dict]], index: str = "hnsw",
                started: float | None = None) -> int:
    """Read-only audit: stored corpus plus upstream field coverage.

    Returns a process exit code so it can gate a scheduled job or CI step.
    """
    from langchain.text_splitter import RecursiveCharacterTextSplitter

    print("\nrebuilding the plan from cached payloads (no database writes)")
    records: list[dict] = []
    if "ebooks" in payloads:
        records += flatten_acts(payloads["ebooks"])
        records += flatten_act_tree(payloads["ebooks"])
    if "blogs" in payloads:
        records += flatten_blogs(payloads["blogs"])
    if "forums" in payloads:
        records += flatten_forums(payloads["forums"])
    if "qna_type1" in payloads or "qna_type2" in payloads:
        records += flatten_qna(payloads.get("qna_type1", []), payloads.get("qna_type2", []))

    by_key: dict[str, dict] = {}
    for r in records:
        by_key.setdefault(r["doc_key"], r)
    records = [r for r in by_key.values() if r["text"].strip()]

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    planned = {r["doc_key"]: len([c for c in splitter.split_text(r["text"]) if c.strip()])
               for r in records}

    state = load_state()
    expected = {
        "embed_model": EMBED_MODEL,
        "embedding_dim": EMBED_DIM,
        "chunk": [CHUNK_SIZE, CHUNK_OVERLAP],
        "hash_scheme": HASH_SCHEME,
        "collection": COLLECTION,
    }
    drift = {k: (state.get(k), v) for k, v in expected.items() if state.get(k) != v}

    if not database_url():
        raise SystemExit("DATABASE_URL is not set. Add it to .env, e.g.\n"
                         "  DATABASE_URL=postgresql+psycopg://rag:rag@127.0.0.1:55432/rag")

    engine = _create_engine(database_url())
    store_report = verify_collection(
        engine, COLLECTION, planned=planned, scope_prefixes=tuple(p for p, _ in _KEY_GROUP),
        dim=EMBED_DIM, expect_ann=index)

    coverage = audit_field_coverage(
        {"ebooks": payloads["ebooks"] if "ebooks" in payloads else [],
         "blogs": payloads["blogs"] if "blogs" in payloads else [],
         "forums": payloads["forums"] if "forums" in payloads else [],
         "qna_type1": payloads.get("qna_type1", []),
         "qna_type2": payloads.get("qna_type2", [])},
        records, qna_key=qna_doc_key)

    print("\n" + store_report.render("stored corpus"))
    print("\n" + coverage.render("upstream field coverage"))
    print(f"\nstate file: {'in sync' if not drift else 'DRIFT ' + repr(drift)}")
    if drift:
        store_report.add("state file matches the running config", False, str(drift))
    else:
        store_report.add("state file matches the running config", True,
                         f"{len(state.get('indexed', {})):,} keys recorded")

    ok = store_report.ok and coverage.ok
    print(f"\nverify: {'PASS' if ok else 'FAIL'}")
    if started is not None:
        print(f"{(time.time() - started) / 60:.1f} min")
    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build the pgvector corpus from the bhumipedia public API.")
    ap.add_argument("--refresh", action="store_true",
                    help="re-fetch the API even if a cached response exists")
    ap.add_argument("--rebuild", action="store_true",
                    help="drop the collection and re-embed everything")
    ap.add_argument("--sources", nargs="+", choices=sorted(SOURCES), default=sorted(SOURCES),
                    help="endpoints to fetch (default: all five)")
    ap.add_argument("--index", default=os.environ.get("PG_INDEX", "hnsw"),
                    choices=["hnsw", "ivfflat", "none"],
                    help="approximate-nearest-neighbour index (default: %(default)s)")
    ap.add_argument("--verify", action="store_true",
                    help="read-only audit of the live collection and field coverage; writes nothing")
    args = ap.parse_args()

    started = time.time()
    print(f"corpus source: {API_BASE}")
    print(f"vector store : collection '{COLLECTION}' on "
          f"{urlsplit(database_url() or 'postgresql://').hostname or '(DATABASE_URL unset)'}"
          f":{urlsplit(database_url() or 'postgresql://').port or 5432}")

    payloads: dict[str, list[dict]] = {}
    for name in args.sources:
        payloads[name] = fetch_source(name, refresh=args.refresh)

    if args.verify:
        sys.exit(verify_only(payloads, index=args.index, started=started))

    with run_lock(RUN_LOCK):
        build(payloads, rebuild=args.rebuild, index=args.index)
    print(f"\ndone in {(time.time() - started) / 60:.1f} min")


if __name__ == "__main__":
    main()
