# Zuora AQuA Query Backend

**Date:** 2026-08-20
**Status:** Approved for planning
**Connector:** `airbyte-source-zuora` (currently 0.2.4)

## Summary

Add a `query_api` config flag selecting between the existing **Data Query**
backend (`POST /query/jobs`) and a new **AQuA** backend
(`POST /v1/batch-query/`, Export ZOQL). Both satisfy one `QueryBackend`
interface; `source.py` keeps slicing and cursor state and stops building
ZOQL itself.

AQuA is not a drop-in replacement. It exposes a different set of objects,
returns CSV instead of JSONL, discovers schemas over an XML API, and uses a
different datetime literal. The flag defaults to Data Query, and switching it
on an existing connection is a breaking change.

## Motivation

Zuora recommended AQuA over the connector's current Data Query approach. The
reasoning behind that recommendation has not been seen in writing; note that
Zuora's own documentation files Export ZOQL and the Describe API under
"Legacy Query Methods". This design proceeds regardless, but the divergence
documented below is worth taking back to Zuora.

## Findings from the sandbox

All figures verified read-only against the APAC Central Sandbox
(`rest.test.ap.zuora.com`, ~3,300 accounts) on 2026-08-20. These are
requirements, not background.

### F1 — Output format is CSV only

`format: "json"` is rejected at submit:

```json
{"success": false, "code": 50000090, "message": "Oops, request is malformed. Please check your request."}
```

Only `csv` (plus the compression variants) works. A type-coercion layer is
therefore mandatory, not optional: CSV carries no types.

### F2 — `useQueryLabels` must be `true`

The flag is inverted relative to its name. For the same query:

| `useQueryLabels` | CSV header |
|---|---|
| absent / `false` | `Account: Account Number,Account: Account Type,Account: Account Balance` |
| `true` | `Account.AccountNumber,Account.AccountType__c,Account.Balance` |

Only `true` produces API names that can be mapped back to the discovered
schema. Every submitted job sets `useQueryLabels: true`.

### F3 — A malformed datetime literal silently returns the whole table

Identical object and lower bound, varying only the literal:

| Literal | Rows |
|---|---|
| `'2026-08-01T00:00:00+10:00'` | 25 |
| `'2026-08-01T00:00:00Z'` | 25 |
| `'2026-08-01'` | 25 |
| `'2026-08-01 00:00:00'` | **3308 (entire table)** |
| `TIMESTAMP '2026-08-01 00:00:00.000000'` | submit-time syntax error |

The space-separated form raises no error — the predicate is silently
dropped. This is the highest-risk behaviour in the API: a formatting slip
turns every incremental slice into a full-table read.

**Requirement:** the AQuA backend renders bounds as ISO-8601 with an explicit
offset via `datetime.isoformat()` (e.g. `2026-08-01T00:00:00+10:00`), and a
unit test asserts the exact rendered predicate string. The renderer is a pure
function so this is cheap to pin.

### F4 — Errors surface at two layers

*Submit-time* (syntax): **HTTP 200** with no `id` in the body.

```json
{"status": "error", "errorCode": "90005",
 "message": "There is a syntax error in one of the queries in the AQuA input (...)"}
```

*Poll-time* (runtime): `status: "aborted"` with detail in
`batches[].message`, e.g. for `BillingPreviewRun`:
`java.lang.RuntimeException: You must specify a select.`

The backend must inspect the submit **body**, not just its status code.

### F5 — `select *` matches the export-context field set exactly

`select * from Account` returned 57 columns; Account has exactly 57 fields
whose `<contexts>` includes `export`. Discovered schemas and emitted records
agree for an object's *own* fields — but see F10: `select *` omits foreign
keys entirely.

`<contexts>` filtering is required: of 2,614 total fields across all
objects, 2,428 carry `export` and 186 do not. Selecting a non-export field
fails the job.

### F6 — Type vocabulary has two gaps in `TYPE_MAPPING`

Across all 119 objects, the export-context field types are:

```
text 1227, datetime 294, picklist 277, decimal 259, date 170,
boolean 118, integer 81, ZOQL 1, number 1
```

`TYPE_MAPPING` in `zuora_client.py` has lowercase `zoql` (Zuora sends
`ZOQL`) and no `number` entry at all. Both currently fall through to
`TYPE_STRING`. Fix: casefold the lookup key and add `number -> TYPE_NUMBER`.
This is a latent bug in the shared map, so the fix benefits both backends.

### F7 — CSV value shapes

- Nulls are empty strings, indistinguishable from genuine empty strings
  (`AdditionalEmailAddresses` was empty in 25/25 rows).
- Booleans are `true` / `false`.
- Decimals are bare: `0`, `-11095`.
- Datetimes are `2026-08-04T20:11:15+1000` — offset **without** a colon, a
  different format from Data Query's. `pendulum.parse` accepts it, so
  existing `stream_slices` state parsing keeps working.

### F8 — Object and field names are case-insensitive in queries

`select id from account` succeeds and still returns canonically-cased
headers (`Account.Id`). Queries may therefore be rendered from the
lowercased stream name, but the backend keeps the canonical-name map from
discovery anyway, for clearer queries and error messages.

### F9 — The two backends expose different tenants

```
Data Query SHOW TABLES : 188 objects
AQuA /v1/describe      : 119 objects
in both (lowercased)   :  85
only Data Query        : 103
only AQuA              :  34
```

Only in Data Query: `orders`, `user`, `attachment`, `chargemetrics`,
`audit*`, `extended*`, `report*`, `workflow*`, `archived_*`, …
Only in AQuA: `order`, `export`, `import`, `invoiceadjustment`,
`invoicesplit`, `journalentrydetail*`, `emailhistory`, …

`orders` -> `order` is the **only** naming variant between the two lists —
checked by comparing singular/plural forms and near-matches across all 135
non-shared names. The remaining 102 Data-Query-only and 33 AQuA-only objects
are genuinely absent from the other backend, so lowercase normalization does
not make the flag transparent.

The `orders` / `order` pair is also not a clean equivalence: AQuA's `Order`
has 24 export fields, all present in Data Query's 26-field `orders`, missing
`accountid` and `invoicescheduleid` (see F10).

### F10 — `select *` omits every foreign key; `<related-objects>` recovers most

This is the most consequential finding. Across 12 sampled core objects,
comparing Data Query's field set to AQuA's own export-context fields:

| Object | Data Query | AQuA `select *` | AQuA + related | FK cols missing | recovered |
|---|---|---|---|---|---|
| Account | 66 | 57 | — | 7 | 0 |
| Subscription | 67 | 54 | — | 12 | 6 |
| Invoice | 67 | 46 | — | 15 | 11 |
| InvoiceItem | 76 | 36 | — | 29 | 23 |
| RatePlan | 20 | 16 | — | 5 | 4 |
| RatePlanCharge | 118 | 101 | — | 12 | 9 |
| Payment | 49 | 46 | — | 5 | 3 |
| CreditMemo | 62 | 40 | — | 10 | 7 |
| ProductRatePlan | 16 | 15 | 16 | 1 | 1 |
| RefundInvoicePayment | 11 | 6 | 23 | 5 | 5 |

`InvoiceItem` drops from 76 fields to 36, and `Subscription` loses
`accountid` — the relational structure of the data would be gone.

In Export ZOQL, foreign keys are not plain columns; they are reached through
the relationship. The per-object describe XML carries a `<related-objects>`
section listing relationship names (1,001 relationships across the 119
objects; `InvoiceItem` has 37). Selecting through them works:

```sql
select *, Account.Id, BillToContact.Id, ShipToContact.Id from Subscription
```

returns 57 columns — the 54 own fields plus `Account.Id`,
`BillToContact.Id`, `ShipToContact.Id`. Crucially the header name maps
straight onto Data Query's: `Account.Id` -> `accountid`,
`BillToContact.Id` -> `billtocontactid`.

**Recovery rate: 66 of 98 missing FK columns (67%).** The 32 that remain
have no corresponding relationship and are unavailable in Export ZOQL at
all — mostly newer eInvoicing/eReporting fields
(`arereportingrequestid`, `einvoicebusinessstatusupdateid`), contact
snapshots (`billtocontactsnapshotid`), and `organizationid`.

An unknown relationship name is a submit-time error
(`"The requested data source could not be found"`), so relationship names
must come from the describe XML and never be guessed.

### F11 — Relationship-derived column names can collide with own fields

For 28 object/relationship pairs, `lower(relationship) + "id"` is already an
own export field — e.g. `InvoiceItem` has its own `subscriptionid` *and* a
`Subscription` relationship. Adding `Subscription.Id` would emit a duplicate
column.

No object has a relationship named after itself, so the prefix in the CSV
header (queried object name for own fields, relationship name for related
ones) is never ambiguous.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Normalize AQuA names to lowercase, stripping the `Object.` prefix | Keeps the 85 shared streams shaped like today's output |
| D2 | Stateless AQuA (`version: "1.0"`), reusing the existing date-window slicing | State stays in Airbyte where it can be inspected and reset; AQuA stateful mode puts it in Zuora, where a failed sync can double-advance and resets need Zuora-side intervention |
| D3 | Each backend advertises its **native** object set | Honest about F9; intersecting to 85 would drop 103 streams that work today |
| D4 | Switching `query_api` on an existing connection is a documented breaking change | F9 plus the incompatible cursor datetime format (F7) mean state and downstream tables do not carry over |
| D6 | Query `select *` **plus** `<Rel>.Id` for every related object whose derived name is not already an own field | F10: `select *` alone loses foreign keys and would strip the data of its relational structure. The guard implements F11 |
| D5 | Skip objects with zero export-context fields at discovery | A general rule that covers `BillingPreviewRun` (F4) without hardcoding a name. `list_objects` returns the full set; the filter applies in `streams()` after schemas are warmed, since the export-field count is only known post-describe |

## Architecture

### Interface

`source.py` currently builds ZOQL itself (`_query_incremental`,
`_query_full`) and passes strings to the client. AQuA needs a different
dialect, naming, and transport, so query construction moves into the
backend:

```python
class QueryBackend(ABC):
    def list_objects(self) -> List[str]:                    ...  # lowercase stream names
    def describe_object(self, name) -> Mapping[str, dict]:  ...  # lowercase field -> JSON type
    def warm_describe_cache(self, names) -> None:           ...
    def read_object(self, name, cursor=None, start=None, end=None) -> Iterator[Mapping]: ...
```

The stream keeps slicing, cursor resolution, and state. The backend owns
dialect, naming, transport, and normalization.

### Modules

The repo uses a flat `source_zuora/` layout; keep it.

| File | Change |
|---|---|
| `zuora_http.py` | **New.** `_request` retry loop and `_sleep_before_retry` extracted verbatim from `zuora_client.py:125-164`; both backends need identical behaviour |
| `zuora_backend.py` | **New.** `QueryBackend` ABC and `get_backend(config)` factory |
| `zuora_describe.py` | **New.** Pure functions parsing `/v1/describe` and `/v1/describe/{object}` XML — no HTTP, so directly unit-testable against captured fixtures |
| `zuora_aqua_client.py` | **New.** `ZuoraAquaClient(QueryBackend)` |
| `zuora_client.py` | Implements `QueryBackend`; gains `read_object` with the query builders moved in from `source.py`; `TYPE_MAPPING` lookup casefolded and `number` added (F6) |
| `source.py` | Uses `get_backend(config)`; `_query_incremental` / `_query_full` removed |
| `spec.json` | New optional `query_api` property |

### AQuA request/response flow

| Step | Call |
|---|---|
| Objects | `GET /v1/describe` -> XML `<objects><object><name>` |
| Fields | `GET /v1/describe/{Object}` -> XML, keep fields whose `<contexts>` includes `export` (F5) |
| Submit | `POST /v1/batch-query/` with `{"format": "csv", "version": "1.0", "encrypted": "none", "useQueryLabels": true, "queries": [{"name": <stream>, "query": <zoql>, "type": "zoqlexport"}]}` |
| Poll | `GET /v1/batch-query/jobs/{id}` -> `status`; on `completed` take `batches[0].fileId` |
| Download | `GET /v1/file/{fileId}`, streamed |

Query rendering — `select *` for own fields, plus one `<Rel>.Id` per related
object, skipping any whose derived column name is already an own field (F11):

```sql
select *, Account.Id, BillToContact.Id, ShipToContact.Id from Subscription
where UpdatedDate >= '2026-08-01T00:00:00+10:00'
  and UpdatedDate <= '2026-08-20T00:00:00+10:00'
order by UpdatedDate asc
```

Relationship names come from the `<related-objects>` section of the object's
describe XML and are never guessed — an unknown name is a submit-time error
(F10). The discovered schema must include the related columns too, so
`describe_object` returns own export fields plus the same derived FK names.

### CSV streaming

`response.iter_lines()` splits on raw newlines and would corrupt quoted
fields containing newlines — plausible in Zuora free-text fields such as
notes and addresses. Instead wrap the raw stream and let `csv` handle
quoting:

```python
reader = csv.DictReader(io.TextIOWrapper(response.raw, encoding="utf-8", newline=""))
```

This streams and parses quoting correctly. It requires `stream=True`, and
because `response.raw` bypasses requests' content decoding, the backend either
passes `decode_content=True` when wrapping or omits `Accept-Encoding: gzip` on
the file request. The unit test for the embedded-newline case pins this.

### Record normalization

Per row, in order:

1. Resolve the column name from its `Prefix.Field` header:
   - prefix **equals the queried object** -> own field; drop the prefix and
     lowercase (`Subscription.AccountNumber` -> `accountnumber`,
     `Account.AccountType__c` -> `accounttype__c`).
   - prefix is **a relationship name** -> foreign key; concatenate both parts
     lowercased (`Account.Id` -> `accountid`,
     `BillToContact.Id` -> `billtocontactid`), matching Data Query's naming.

   No object has a self-named relationship (F11), so the two cases never
   overlap.
2. Map `""` -> `None` (F7).
3. Coerce to the Describe-derived type: `decimal`/`number`/`integer` ->
   numeric, `boolean` -> bool, everything else passthrough as string.
   Related `*.Id` columns are always string.

Datetimes stay strings, matching the Data Query backend and keeping the
cursor comparison in `source.py` unchanged.

### Error mapping

| Condition | Raised |
|---|---|
| Submit body has `status: "error"` / no `id` (F4) | `ZOQLQueryFailed(message, query)` |
| Job `aborted` with a transient marker | `ZuoraTransientError` — retried by the existing job-level loop |
| Job `aborted` with `You must specify a select` | `ZOQLQueryCannotProcessObject` — stream skipped with a warning, as today |
| HTTP 401/403 | `ZuoraConfigError` (existing behaviour in `zuora_http.py`) |

## Config

```json
"query_api": {
  "title": "Query API",
  "type": "string",
  "description": "Which Zuora API to extract with. `Data Query` uses ZOQL Data Query jobs. `AQuA` uses the Aggregate Query API with Export ZOQL, which exposes a different set of objects — switching this on an existing connection requires a fresh sync.",
  "enum": ["Data Query", "AQuA"],
  "default": "Data Query"
}
```

Not added to `required`, so existing saved configs remain valid and default
to today's behaviour. `data_query` (Live/Unlimited) applies only to the Data
Query backend; its description says so rather than restructuring the spec
into a `oneOf`, which would invalidate saved configs.

## Testing

Unit tests with a stubbed session, mirroring `unit_tests/test_client.py`:

- **Describe XML parsing** against captured fixtures: object list, field
  list, `<contexts>` filtering keeps `export` and drops the rest.
- **Type mapping**: `ZOQL` and `number` resolve correctly (F6 regression).
- **Query rendering**: exact predicate string for an incremental slice, and
  the bare `select * from X` for full refresh. Directly guards F3.
- **CSV normalization**: prefix stripping, lowercasing, `""` -> `None`,
  numeric and boolean coercion, and a quoted field containing a newline.
- **Job lifecycle**: submit-body error detection (F4), `aborted` -> transient
  vs cannot-process classification, job-level retry on transient abort.
- **Discovery**: objects with zero export fields are skipped (D5).
- **Related-object handling** (F10/F11): the rendered select list includes
  `<Rel>.Id` for each related object; a relationship whose derived name
  duplicates an own field is omitted; `Account.Id` normalizes to `accountid`
  while `Subscription.Id` on a Subscription query normalizes to `id`; the
  discovered schema contains the derived FK names.
- **Factory**: `get_backend` selects on `query_api`, defaulting to Data Query
  when the key is absent.

Integration verification before trusting the backend: one real sync of a
shared stream (e.g. `account`) against the sandbox, checking that record
count matches the equivalent Data Query sync and that the second sync reads
strictly fewer rows than the first.

## Risks

| Risk | Mitigation |
|---|---|
| Silent full-table reads from a malformed literal (F3) | Pinned renderer, unit test on the exact predicate, second-sync row-count check |
| AQuA is a legacy API and may be deprecated | Data Query stays the default; the flag is additive and removable |
| CSV loses the null/empty-string distinction (F7) | Accepted: empty strings become `None`; documented as a behavioural difference from Data Query |
| Users flip the flag expecting a transparent swap (F9) | Spec description and README both state it needs a fresh sync |
| 32 FK columns per the sampled 12 objects are unavailable in Export ZOQL at all (F10) | Accepted and documented; affects mostly eInvoicing/eReporting and contact-snapshot FKs. Quantify across all 119 objects during implementation |
| Concurrent AQuA export limits on large tenants | Reuse the bounded `describe_concurrency` pool pattern already in `warm_describe_cache` |
