"""Integrity checks for the pgvector corpus.

`ingest.py` reports success only when `verify_collection()` finds no problem, so
a truncated API response, a partially applied batch write or a leftover chunk
fails the run loudly instead of quietly shrinking the index. The same function
backs `ingest.py --verify` for a read-only audit of a collection already serving
queries, and `audit_field_coverage()` is the standing guard against an upstream
field being added and never indexed.
"""

from __future__ import annotations

import json
import html
import os
import re
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

# Without these a row cannot be traced back to a record, cited, or re-purged.
REQUIRED_META = ("doc_key", "source_type", "chunk_index", "chunk_total")

# Upstream fields that are deliberately not indexed, as bare names (exempt in
# every source) or as `source.field` when the exemption is source specific.
# Every entry is non-content: CMS bookkeeping, counters, image assets or
# structural ids. A field absent from this list and absent from the records is
# data loss, so adding an upstream field without indexing it fails the audit.
EXEMPT_FIELDS = {
    # structural ids, present at every nesting level of the act tree
    "id",
    "act_id",
    "section_id",
    "subsection_id",
    "schedule_id",
    "subschedule_id",
    "total_number_of_section",
    "total_number_of_sub_section",
    "total_number_of_schedules",
    "total_number_of_subschedules",
    # CMS workflow and authorship
    "created_by",
    "updated_by",
    "created_at",
    "updated_at",
    "deleted_at",
    "published_at",
    "modified_date",
    "status",
    "is_active",
    "is_published",
    "slug",
    "owner",
    "meta_keywords",
    "multiple_reference_link",
    "meta_description",
    "meta_title",
    "seo_title",
    "seo_description",
    # counters
    "like_user_counter",
    "share_user_counter",
    "viewer_counter",
    "comment_counter",
    "like_count",
    "view_count",
    "reply_count",
    "member_count",
    "topic_count",
    "download_count",
    "is_pinned",
    "group_type",
    "badge",
    # binary assets and identifiers, never legal text
    "cover",
    "cover_image",
    "cover_image_url",
    "thumbnail",
    "bar_code",
    "barcode",
    "file_size",
    "file_name",
    "extension",
    "mime_type",
    "uploaded_by",
    "book_tags",
    "tags",
    "featured",
    "pinned",
    "sticky",
    # ebooks carry CMS bookkeeping that the other sources do not
    "ebooks.created_date",
    "ebooks.created_at_bn",
    "ebooks.created_at_en",
    "ebooks.publication_date_en",
}

_TAG = re.compile(r"<[^>]+>")
_BREAK = re.compile(r"(?i)<(br|/p|/div|/li|/h[1-6])\s*/?>")
_WS = re.compile(r"\s+")


def _strip_html(value) -> str:
    """Same reduction the ingest applies, so a raw payload field is comparable
    with the cleaned text that was actually indexed."""
    text = html.unescape(str(value))
    text = _BREAK.sub("\n", text)
    text = _TAG.sub(" ", text)
    return html.unescape(text)


class IntegrityError(RuntimeError):
    """The stored corpus does not match what was intended."""


def norm(value) -> str:
    return _WS.sub(" ", unicodedata.normalize("NFKC", _strip_html(value))).strip().casefold()


# --------------------------------------------------------------------------- #
# report
# --------------------------------------------------------------------------- #

@dataclass
class Report:
    checks: list[tuple[str, bool, str]] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    def add(self, name: str, ok, detail: str = "") -> bool:
        self.checks.append((name, bool(ok), detail))
        return bool(ok)

    @property
    def problems(self) -> list[str]:
        return [f"{name}: {detail}" for name, ok, detail in self.checks if not ok]

    @property
    def ok(self) -> bool:
        return not self.problems

    def render(self, title: str = "integrity") -> str:
        head = f"{title}: {'PASS' if self.ok else 'FAIL'}"
        if self.stats:
            head += "  " + "  ".join(f"{k}={v}" for k, v in self.stats.items())
        lines = [head]
        for name, ok, detail in self.checks:
            mark = "PASS" if ok else "FAIL"
            lines.append(f"  [{mark}] {name}" + (f" - {detail}" if detail else ""))
        return "\n".join(lines)

    def raise_if_failed(self, title: str = "integrity") -> None:
        if not self.ok:
            body = "\n".join(f"  - {p}" for p in self.problems)
            raise IntegrityError(f"{title} check failed:\n{body}")


# --------------------------------------------------------------------------- #
# single-writer lock
# --------------------------------------------------------------------------- #

@contextmanager
def run_lock(path: Path, label: str = "ingest"):
    """Guarantee a single writer.

    The lock is held by the OS on the open file, so it is released even if the
    process is killed - a stale lock file left by a crash cannot block the next
    run, which a pid check alone would do.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_bytes(b"0")

    handle = open(path, "r+b")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise IntegrityError(
            f"another {label} run is already writing the index (holding "
            f"{path.name}). Wait for it to finish, or delete the file if you are "
            f"sure no run is active."
        ) from exc

    try:
        yield
    finally:
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        handle.close()


# --------------------------------------------------------------------------- #
# collection verification
# --------------------------------------------------------------------------- #

_SCOPE_SQL = """
SELECT e.cmetadata->>'doc_key'
FROM langchain_pg_embedding e
JOIN langchain_pg_collection c ON e.collection_id = c.uuid
WHERE c.name = %s AND e.cmetadata->>'doc_key' LIKE ANY(%s)
"""


def verify_collection(
    engine,
    collection: str,
    *,
    planned: dict[str, int] | None = None,
    scope_prefixes: tuple[str, ...] | None = None,
    dim: int | None = None,
    expect_ann: str = "hnsw",
) -> Report:
    """Check the stored collection against what was meant to be written.

    `planned` maps doc_key -> expected chunk count. When given together with
    `scope_prefixes`, records outside the scope are ignored so a partial-source
    run does not report untouched groups as orphans.
    """
    rep = Report()

    with engine.connect() as conn:
        one = lambda sql, params=(): conn.exec_driver_sql(sql, params).one()

        total, distinct = one(
            "SELECT count(*), count(DISTINCT e.cmetadata->>'doc_key') "
            "FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s", (collection,)
        )
        rep.stats["chunks"] = f"{total:,}"
        rep.stats["records"] = f"{distinct:,}"

        rep.add("collection is not empty", total > 0, f"{total:,} rows")

        bad_doc = one(
            "SELECT count(*) FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s AND (e.document IS NULL OR btrim(e.document) = '')",
            (collection,)
        )[0]
        rep.add("every row has retrievable text", bad_doc == 0,
                f"{bad_doc:,} rows with null/blank document")

        missing_keys = one(
            "SELECT count(*) FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s AND (e.cmetadata IS NULL OR "
            "  NOT (e.cmetadata ?& %s))",
            (collection, list(REQUIRED_META))
        )[0]
        rep.add("every row carries required metadata", missing_keys == 0,
                f"{missing_keys:,} rows missing any of {', '.join(REQUIRED_META)}")

        null_emb = one(
            "SELECT count(*) FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s AND e.embedding IS NULL", (collection,)
        )[0]
        rep.add("no null embeddings", null_emb == 0, f"{null_emb:,} rows")

        if dim:
            wrong = one(
                "SELECT count(*) FROM langchain_pg_embedding e "
                "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
                "WHERE c.name = %s AND vector_dims(e.embedding) IS DISTINCT FROM %s",
                (collection, dim)
            )[0]
            rep.add(f"all embeddings are {dim}-dim", wrong == 0, f"{wrong:,} rows")

        # A zero vector is silently unmatchable: cosine similarity to it is
        # undefined or zero, so the chunk can never surface. e5 output is
        # L2-normalised, so a healthy row has squared norm 1.
        zero = one(
            "SELECT count(*) FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s AND -(e.embedding <#> e.embedding) < 0.5",
            (collection,)
        )[0]
        rep.add("no zero/degenerate embeddings", zero == 0,
                f"{zero:,} rows with squared norm < 0.5")

        bad_span, bad_total, dup_idx = one(
            "WITH per AS ("
            "  SELECT e.cmetadata->>'doc_key' AS dk, count(*) AS n,"
            "         min((e.cmetadata->>'chunk_index')::int) AS lo,"
            "         max((e.cmetadata->>'chunk_index')::int) AS hi,"
            "         count(DISTINCT (e.cmetadata->>'chunk_index')::int) AS uniq,"
            "         max((e.cmetadata->>'chunk_total')::int) AS tot"
            "  FROM langchain_pg_embedding e"
            "  JOIN langchain_pg_collection c ON e.collection_id = c.uuid"
            "  WHERE c.name = %s GROUP BY 1)"
            "SELECT count(*) FILTER (WHERE tot IS DISTINCT FROM n),"
            "       count(*) FILTER (WHERE lo IS DISTINCT FROM 0 OR hi IS DISTINCT FROM n - 1),"
            "       count(*) FILTER (WHERE uniq IS DISTINCT FROM n)"
            "FROM per", (collection,)
        )
        rep.add("chunk_index is contiguous from 0", bad_span == 0,
                f"{bad_span:,} records with a gap or wrong span")
        rep.add("chunk_total matches stored chunks", bad_total == 0,
                f"{bad_total:,} records disagreeing with their own chunk_total")
        rep.add("no duplicate chunk_index", dup_idx == 0,
                f"{dup_idx:,} records with repeated indices")

        census = conn.exec_driver_sql(
            "SELECT e.cmetadata->>'source_type', count(*) "
            "FROM langchain_pg_embedding e "
            "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
            "WHERE c.name = %s GROUP BY 1 ORDER BY 2 DESC", (collection,)
        ).all()
        rep.stats["types"] = len(census)
        rep.add("every row has a source_type",
                all(r[0] for r in census),
                ", ".join(f"{k}={v:,}" for k, v in census[:4]))

        if expect_ann and expect_ann != "none":
            found = one(
                "SELECT count(*) FROM pg_indexes "
                "WHERE tablename = 'langchain_pg_embedding' "
                "  AND indexdef ILIKE '%%USING " + expect_ann + "%%'"
            )[0]
            rep.add(f"{expect_ann} index present", found > 0,
                    f"{found} matching index(es)")

        if planned is not None:
            if not scope_prefixes:
                scope_prefixes = tuple(planned)
            patterns = [p + "%" for p in scope_prefixes]
            stored = {r[0] for r in conn.exec_driver_sql(
                _SCOPE_SQL, (collection, patterns)).all() if r[0]}
            want = set(planned)
            absent = sorted(want - stored)
            extra = sorted(stored - want)
            rep.add("every planned record is stored", not absent,
                    f"{len(absent)} missing" + (f": {absent[:5]}" if absent else ""))
            rep.add("no orphan records in scope", not extra,
                    f"{len(extra)} unexpected" + (f": {extra[:5]}" if extra else ""))
            wrong_count = []
            if stored and not absent:
                actual = dict(conn.exec_driver_sql(
                    "SELECT e.cmetadata->>'doc_key', count(*) "
                    "FROM langchain_pg_embedding e "
                    "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
                    "WHERE c.name = %s AND e.cmetadata->>'doc_key' = ANY(%s) "
                    "GROUP BY 1", (collection, list(want))).all())
                wrong_count = [k for k, n in planned.items() if actual.get(k) != n]
                rep.add("chunk count matches the plan", not wrong_count,
                        f"{len(wrong_count)} records differ from plan"
                        + (f": {wrong_count[:5]}" if wrong_count else ""))

    return rep


# --------------------------------------------------------------------------- #
# upstream field coverage
# --------------------------------------------------------------------------- #

def _index_records(records: list[dict]) -> dict:
    """Map upstream rows to every record they produced.

    Four lookups are needed. An act row yields an `act:` header and an
    `act_text:` body, and its `sections` tree becomes thousands of separate
    section/subsection/schedule records reachable only through `act_id`. A forum
    row holds its topics inline, so the topic records are reachable only through
    `group_id`. Q&A rows have content-hash doc_keys and are matched on the
    question and answer together, because the two tables repeat one question
    with different answers. Checking a row against only its own header would
    report every field living in a nested structure as lost.
    """
    by_id: dict[str, list[dict]] = {}
    by_act: dict[object, list[dict]] = {}
    by_group: dict[object, list[dict]] = {}
    by_qa: dict[str, dict] = {}
    for r in records:
        key = r["doc_key"]
        if key.startswith("qna:"):
            by_qa[key] = r
            continue
        prefix, _, ident = key.partition(":")
        if prefix in ("act", "act_text", "blog", "forum_group", "forum_topic"):
            by_id.setdefault(f"{prefix}:{ident.split(':', 1)[0]}", []).append(r)
        elif prefix in ("section", "subsection", "schedule", "subschedule"):
            by_act.setdefault(r["meta"].get("act_id"), []).append(r)
        if prefix == "forum_topic":
            by_group.setdefault(r["meta"].get("group_id"), []).append(r)
    return {"id": by_id, "act": by_act, "group": by_group, "qa": by_qa}


def _exempt(source: str, path: str) -> bool:
    leaf = path.rsplit(".", 1)[-1]
    return (path in EXEMPT_FIELDS or leaf in EXEMPT_FIELDS
            or f"{source}.{path}" in EXEMPT_FIELDS)


def _leaves(source: str, value, path: str = ""):
    """Yield (dotted path, text) for every non-exempt leaf of a payload row.

    The key names are kept at every level: the act tree nests bookkeeping ids and
    counters alongside real content, and flattening them together would either
    demand the ids be indexed or excuse the content not being.
    """
    if isinstance(value, dict):
        for key, sub in value.items():
            child = f"{path}.{key}" if path else str(key)
            if _exempt(source, child):
                continue
            yield from _leaves(source, sub, child)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _leaves(source, item, path)
    else:
        text = norm(value)
        if len(text) >= 3:
            yield path, text


def audit_field_coverage(groups: dict[str, list[dict]], records: list[dict],
                         sample: int = 400, qna_key=None) -> Report:
    """Prove every non-exempt upstream field reaches a record.

    `groups` maps source name -> raw payload rows. Values are compared in the
    same cleaned, case-folded form the ingest stores, against the text and
    metadata of every record that row produced. A field is fully covered only if
    every sampled value was found; a partial result means some rows lose it.

    `qna_key` must return the doc_key a Q&A row would produce, since those keys
    are content hashes and cannot be rebuilt here without duplicating the
    ingest's normalisation.
    """
    rep = Report()
    index = _index_records(records)
    seen: dict[str, dict] = {}
    checked = 0
    unmatched = 0

    for name, rows in groups.items():
        step = max(1, len(rows) // sample) if sample else 1
        for row in rows[::step]:
            ident = row.get("id")
            matches: list[dict] = []
            for prefix in ("act", "act_text", "blog", "forum_group", "forum_topic"):
                matches.extend(index["id"].get(f"{prefix}:{ident}", ()))
            if name == "ebooks":
                matches.extend(index["act"].get(ident, ()))
            if name == "forums":
                matches.extend(index["group"].get(ident, ()))
            if not matches and name.startswith("qna") and qna_key:
                hit = index["qa"].get(qna_key(row))
                matches = [hit] if hit else []
            if not matches:
                unmatched += 1
                continue
            checked += 1
            haystack = norm(" ".join(
                r["text"] + " " + json.dumps(r["meta"], ensure_ascii=False)
                for r in matches))
            for path, text in _leaves(name, row):
                slot = seen.setdefault(f"{name}.{path}",
                                       {"hit": 0, "miss": 0, "sample": ""})
                if text[:200] in haystack:
                    slot["hit"] += 1
                else:
                    slot["miss"] += 1
                    slot["sample"] = slot["sample"] or text[:60]

    rep.stats["rows_checked"] = f"{checked:,}"
    rep.stats["fields"] = f"{len(seen)}"
    rep.add("sampled rows all map to a record", unmatched == 0,
            f"{checked:,} matched, {unmatched:,} unmatched")

    unindexed = {k: v for k, v in seen.items() if v["hit"] == 0}
    partial = {k: v for k, v in seen.items() if 0 < v["hit"] < v["hit"] + v["miss"]}
    rep.add("every non-exempt field is indexed", not unindexed,
            "; ".join(f"{k} (e.g. {v['sample']!r})" for k, v in list(unindexed.items())[:6]))
    rep.add("fields are fully covered, not partly", not partial,
            "; ".join(f"{k} {v['miss']}/{v['hit'] + v['miss']}" for k, v in list(partial.items())[:6]))
    return rep
