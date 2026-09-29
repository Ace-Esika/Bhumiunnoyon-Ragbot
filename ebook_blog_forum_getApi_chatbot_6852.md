# Full-Details APIs (Approved-Only)

Three read-only GET endpoints that each return **every approved record** for
their app, with the **complete field set** (not the trimmed list/detail
serializers used elsewhere) plus any nested content. They exist so a
consumer can fetch a full dataset in one call instead of paging through the
list endpoints and then hitting a detail endpoint per record.

## Common behavior

| | |
|---|---|
| Method | `GET` only (no create/update/delete) |
| Auth | `AllowAny` — public, unauthenticated |
| Pagination | None — response is a plain JSON array |
| Null fields | Any field whose value is `None` is **omitted** from that object. Two records of the same type can therefore have different keys. |
| Approval | Only `approved=True` (and non-soft-deleted) records are returned |

---

## 1. `GET /api/ebooks/full/`

All approved ebooks/acts, across every `ebooks_type` (আইন, অধ্যাদেশ,
রাষ্ট্রপতির আদেশ, বিধিমালা, প্রবিধান, নীতিমালা, নির্দেশিকা, পরিপত্র,
প্রজ্ঞাপন, অফিস আদেশ, সমঝোতা স্মারক, ম্যানুয়াল, গেজেট, অন্যান্য) — this is
the superset of `/api/acts/`, `/api/ordinances/`, `/api/rules/`, etc.

- **View**: `acts.acts_api_views.FullActsList`
- **Filters**: `is_soft_deleted=False, approved=True, is_draft=False`
- **Ordering**: `act_year` desc (nulls last), then `publication_date` desc
- **Performance**: the whole `sections → subsections → schedules →
  subschedules` tree is loaded with `prefetch_related`
  (`_build_full_acts_prefetch`), so the list costs a fixed number of
  queries regardless of how many acts/sections it returns.

### Top-level fields

| Field | Notes |
|---|---|
| `id` | |
| `ebooks_type` | e.g. `আইন`, `অধ্যাদেশ`, `বিধিমালা`, ... |
| `act_year`, `number` | |
| `title_of_act` | |
| `publication_date`, `publication_by` | `publication_by` is `গেজেট` or `নন-গেজেট` |
| `proposal`, `objective`, `motto` | free text |
| `file`, `merged_file`, `system_generated_pdf`, `cover` | file/image URLs |
| `schedules` | free-text field on the Act itself — **not** the nested schedule objects (see below) |
| `bar_code` | |
| `created_at_bn`, `created_at_en`, `applicable_date_bn`, `applicable_date_en` | |
| `heading`, `branch`, `sub_branch` | |
| `signature_position`, `signature_by`, `created_by`, `copy_to`, `footer` | |
| `meta_keywords`, `multiple_reference_link` | arrays of strings |
| `created_date` | |
| `sections` | nested array, see below |

Left out on purpose (internal workflow/bookkeeping, not useful to API
consumers): `featured`, `is_book`, `status`, `modified_type`,
`modified_act`, `modified_year`, `live`, `is_draft`, `draft_by`,
`draft_date`, `approved`, `approved_by`, `approved_date`, `is_proof_read`,
`proof_read_by`, `proof_read_date`, `is_disallowed`, `disallow_reason`,
`is_soft_deleted`, `soft_deleted_by`, `soft_deleted_date`,
`total_number_of_sections`, `total_number_of_sub_sections`,
`total_number_of_schedules`, `total_number_of_sub_schedules`, all
`repealed_*` / `amendment_*` / `appendment_*` relation fields,
`total_number_of_section`, `like_user_counter`, `share_user_counter`,
`viewer_counter`, `comment_counter`, `owner`.

### Nested content shape

```
sections: [
  {
    id, act_id, number, heading, content, note, total_number_of_sub_section,
    subsections: [
      {
        id, act_id, section_id, number, heading, content, note,
        total_number_of_schedules,
        schedules: [                       // schedules under this subsection
          {
            id, act_id, section_id, sub_section_id, number, heading, content,
            note, total_number_of_sub_schedules,
            subschedules: [
              { id, act_id, section_id, sub_section_id, schedule_id, number, heading, content, note }
            ]
          }
        ]
      }
    ],
    schedules: [ /* schedules attached directly to the section, same shape as above */ ]
  }
]
```

### Example

```json
[
  {
    "id": 838,
    "ebooks_type": "আইন",
    "act_year": "২০২৩",
    "number": "১২",
    "title_of_act": "উদাহরণ আইন, ২০২৩",
    "publication_date": "০১-০১-২০২৩",
    "publication_by": "গেজেট",
    "cover": "/uploads/Cover/Act/example.jpg",
    "created_date": "2023-01-01T10:00:00Z",
    "sections": [
      {
        "id": 4021,
        "act_id": 838,
        "number": "১",
        "heading": "সংক্ষিপ্ত শিরোনাম",
        "content": "এই আইন ... নামে অভিহিত হইবে।",
        "subsections": [],
        "schedules": []
      }
    ]
  }
]
```

---

## 2. `GET /api/blogs/full/`

All approved blogs, with every content field on the `Blog` model.

- **View**: `blogs.blogs_api_views.BlogFullList`
- **Serializer**: `blogs.serializers.BlogFullSerializer`
- **Filters**: `approved=True, is_soft_deleted=False`
- **Ordering**: `-created_date`

### Fields

| Field | Notes |
|---|---|
| `id` | |
| `title_name` | |
| `author` | |
| `cover` | image URL |
| `featured` | |
| `content` | HTML (TinyMCE) |
| `like_user_counter`, `share_user_counter`, `viewer_counter`, `comment_counter` | |
| `created_date` | |

Left out: `approved`, `is_soft_deleted`, `soft_deleted_by`,
`soft_deleted_date`, `owner`.

### Example

```json
[
  {
    "id": 12,
    "title_name": "উদাহরণ ব্লগ",
    "author": "রহিম উদ্দিন",
    "cover": "/uploads/blog/cover/example.jpg",
    "featured": true,
    "content": "<p>ব্লগের বিস্তারিত বিষয়বস্তু...</p>",
    "like_user_counter": 24,
    "viewer_counter": 310,
    "created_date": "2024-05-10T08:15:00Z"
  }
]
```

---

## 3. `GET /api/forums/full/`

All approved, **open** forums (`Group` model — `Forum` is an alias of the
same model), each with its topics nested underneath. Private/closed
(`group_type='close'`) groups are excluded, same as the existing
`/api/groups/` list.

- **View**: `forum.forums_api_views.FullGroupsList`
- **Serializer**: `forum.serializers.FullGroupSerializer` /
  `FullTopicSerializer`
- **Filters**: `approved=True, is_soft_deleted=False, group_type='open'`
- **Ordering**: groups by `-created_date`; nested topics by `-is_pinned,
  -created_date`
- **Performance**: topics are loaded via `prefetch_related`
  (`_build_full_groups_prefetch`) — one extra query total, not one per group.

### Group fields

| Field | Notes |
|---|---|
| `id` | UUID |
| `name`, `description` | |
| `thumbnail` | image URL |
| `category` | FK id of the group's `MainCategory` |
| `badge` | `none` / `official` / `verified` |
| `group_type` | always `open` for this endpoint |
| `featured` | |
| `member_count`, `topic_count` | |
| `created_date` | |
| `topics` | nested array, see below |

Left out: `approved`, `owner`, `allowed_users`, `is_soft_deleted`,
`soft_deleted_by`, `soft_deleted_date`.

### Topic fields (nested under `topics`)

| Field | Notes |
|---|---|
| `id` | UUID |
| `group` | parent group id (UUID) |
| `title`, `description` | |
| `thumbnail` | image URL |
| `status` | `open` / `hold` / `closed` / `resume` |
| `is_pinned`, `is_archived` | |
| `view_count`, `reply_count`, `like_count` | |
| `created_date` | |

Note: topics are included regardless of `status` — only
`is_soft_deleted=False` is applied. Topic replies are **not** nested here.

### Example

```json
[
  {
    "id": "b3b2a6b0-1e2d-4a9f-8c3e-7a2f0e9d1c44",
    "name": "উদাহরণ ফোরাম",
    "description": "<p>ফোরামের বিবরণ</p>",
    "badge": "verified",
    "group_type": "open",
    "member_count": 128,
    "topic_count": 2,
    "created_date": "2024-02-01T09:00:00Z",
    "topics": [
      {
        "id": "9f1c2e3a-4b5d-6e7f-8a9b-0c1d2e3f4a5b",
        "group": "b3b2a6b0-1e2d-4a9f-8c3e-7a2f0e9d1c44",
        "title": "প্রথম আলোচনা",
        "status": "open",
        "is_pinned": true,
        "view_count": 45,
        "reply_count": 3,
        "like_count": 7,
        "created_date": "2024-02-02T11:30:00Z"
      }
    ]
  }
]
```

---

## Implementation in this repository

All three endpoints are implemented by `public_api.py` and served by
`server.py`.

```
python server.py --sql     # from the bundled .sql snapshot, no DB needed
python server.py           # from live PostgreSQL (DATABASE_URL in .env)
```

| | |
|---|---|
| Live deployment | `https://bhumipedia.land.gov.bd` |
| Local (default port) | `http://127.0.0.1:8790` |

Full URLs: `{base}/api/ebooks/full/`, `{base}/api/blogs/full/`,
`{base}/api/forums/full/` (a missing trailing slash is also accepted).

The source is the same `{table: {cols, rows}}` dict that
`build_db.load_tables_from_sql()` and `db_source.load()` produce, so the same
code serves the `.sql` snapshot and a live database. The collections are
rendered once per reload rather than per request, since they take no query
parameters.

### Verified against the live deployment

Key sets and types were probed field-by-field against
`https://bhumipedia.land.gov.bd`, and the documented filters were confirmed to
reproduce the live row counts:

| endpoint | live rows | local (`--sql`) | key sets |
|---|---|---|---|
| `/api/ebooks/full/` | 153 | 130 | identical |
| `/api/blogs/full/` | 21 | **21** | identical (11 keys) |
| `/api/forums/full/` | 2 groups / 4 topics | **2 groups / 4 topics** | identical (9 + 12 keys) |

Nested totals locally: 130 acts, 1,278 sections, 3,185 subsections, 886
schedules, 64 subschedules.

The pg_dump/`psycopg2` layer hands every value over as text, so `public_api`
coerces per declared field kind: `t`/`f` → JSON booleans, counters → integers,
`{a,b}` PostgreSQL array literals → JSON arrays, and timestamps
(`2025-09-04 12:26:57.599859+00`) → ISO-8601 (`2025-09-04T12:26:57.599859+00:00`).

Child rows are bucketed with dict indexes rather than queried per parent, so
each collection costs a fixed number of passes regardless of size.

### Deviations from the live deployment

1. **Row counts differ because the bundled `.sql` snapshot is older than the
   live database.** The filters themselves are confirmed correct: they
   reproduce the live counts exactly for blogs (21) and forums (2). Acts are
   130 locally vs 153 live because the dump predates the newer approved acts.
2. **`note` exists only on `acts_sections` in this schema.** Subsections,
   schedules and subschedules have no such column, so those nested objects
   cannot carry the `note` key listed above. It is in the allowlist for
   sections and is emitted whenever non-null.
3. **Some keys are absent purely because the local values are null.**
   Null-omission means the key set varies per record: locally every group has
   a null `description`, `badge` and `category`, and every topic a null
   `thumbnail`, so those keys do not appear. They are in the allowlist and
   will appear as soon as the data is populated.
4. **`act_year` is stored inconsistently** in the source — some rows use
   Bengali numerals (`২০২৬`), some ASCII (`2026`). Ordering converts both to
   integers so the `act_year` desc / `publication_date` desc sort is correct,
   but the response echoes each row's original representation rather than
   rewriting it.
