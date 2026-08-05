# org-apache-arrow

**An Arrow IPC reader in portable `.cljc`**, providing
[`columnar`](https://github.com/kotoba-lang/columnar)'s `IColumnSource`. No
Rust, no JNI, no native library, no generated FlatBuffers bindings — the
format is decoded from bytes.

```clojure
(require '[arrow.source :as ar] '[columnar.plan :as plan])

(plan/scan (ar/open bytes) {:columns ["price"] :predicates [[:= "price" 120]]})
;; => {:rows [{::plan/row 4 "price" 120}] :chunks-read 3 :chunks-skipped 0}
```

Origin plane: the format is Apache's, so the repo is named for where it comes
from (`arrow.apache.org` → `org-apache-arrow`), not for what it does here.

## Why this repo exists

`columnar` was written as the seam a file format plugs into, and until now
exactly one format was plugged into it. With a sample size of one, *"the seam
is not Parquet-shaped"* was a claim about code that had never been asked the
question.

Arrow IPC is the right second format precisely because it is **not** a second
Parquet. Its metadata is FlatBuffers rather than Thrift compact, and its data
layer is *simpler* than Parquet's — uncompressed by default, no encoding
layer, just a validity bitmap beside a values buffer. So the pressure it puts
on the seam comes from a different direction.

**The answer: `columnar` did not change.** The protocol was implemented as
written. What did change is one thing, described below.

## What Arrow records, and what it does not

| question | Parquet | Arrow IPC |
|---|---|---|
| rows per chunk | footer | batch metadata |
| nulls per column chunk | footer statistics | `FieldNode.null_count` |
| **min / max per column chunk** | footer statistics | **not recorded** |

Arrow has no column statistics. That is a property of the format, not a gap in
this reader, and it is the most interesting thing about plugging it in:
`columnar.source` documents `-chunk-stats` returning `nil`-or-unbounded as
*"the source makes no claim"*, and `columnar.stats` states as its first rule
that **absent statistics never permit a skip**. Both were written before there
was a format that exercised them.

So an Arrow source reports `:rows` and `:nulls` and no bounds, and the
consequences are visible rather than hidden:

```clojure
(agg/aggregate s {:agg :count})            ;; {:value 9 :from :statistics :read 0}
(agg/aggregate s {:agg :count-non-null …}) ;; {:value 2 :from :statistics :read 0}
(agg/aggregate s {:agg :max :column "price"})
;; {:value 230 :from :scan :read 3}   <- Parquet answers this with :read 0
```

A format that records less plugs in by **reporting** less, not by growing a
special case. That is the property this repo was written to check.

### The one thing that did move

`IByteSource` — the range-reading seam — lived in `parquet.bytes`. A second
format needing the identical protocol made it shared rather than Parquet's, so
it is now **`columnar.bytes`**, beside `IColumnSource`, which is the other
half of the same seam. Two things that can substitute for each other are one
thing (ADR-2607299700).

## Metadata survives buffers this reader cannot decode

The single most useful property, and the same one `org-apache-parquet` holds
for statistics. Body compression is a property of the **buffers**; batch
lengths and null counts live in the **metadata**, which is never compressed.

So a zstd-compressed file still answers `count`, still answers
`count-non-null`, and still prunes all-null chunks, reading nothing but
message headers — while `-read-column` refuses it and **names the codec**.
`compressed.arrow` is the fixture that holds that line, and there is a test
asserting both halves.

## What it decodes, and what it refuses by name

```
decoded
  int8 / int16 / int32 / int64      uint8 / uint16 / uint32 / uint64
  float32 / float64                 bool (bit-packed)
  utf8 / large_utf8                 binary / large_binary
  null
  multiple record batches, each a prunable chunk
  the pre-0.15 envelope (no continuation marker) as well as the current one

refused, by name
  body compression:  lz4_frame / zstd
  dictionary-encoded fields
  nested:  list / large_list / struct / map / union / fixed_size_list
           run-end-encoded, and the view types
  decimal / date / time / timestamp / interval / duration
```

A refused *type* still lists in the schema — refusing to name a file's columns
is a worse failure than refusing to decode one of them — and a refused *layout*
throws when the source is opened rather than when the bad column is asked for,
because an unknown buffer count desynchronises every column after it.

## Reads are ranges

Opening a file costs the footer: a 10-byte tail, then the footer FlatBuffer it
points at. Every batch offset, batch row count and column name comes out of
that, so pruning a 40 GB object costs the footer and nothing more.

Reading a column fetches that column's buffers at their own offsets and
lengths — not the batch body. There is a test that asserts it by metering the
transport, because an assertion on the *answer* cannot tell a projected read
apart from reading everything and slicing.

## Portability, and 64-bit integers

Portable `.cljc`: the JVM and ClojureScript, with the Worker (cljs) being the
runtime this actually deploys on.

`arrow.flatbuffers/i64` is **exact or nothing**. The obvious accumulation with
`Math/pow` puts the top two bytes of a 64-bit value past double precision and
returns a quietly rounded answer — on the JVM as well as in ClojureScript. So
the JVM path is exact integer arithmetic, and the ClojureScript path *refuses*
a magnitude it cannot represent rather than rounding it. A reader that returns
a plausible wrong number is worse than one that says it cannot represent this
value, because nothing downstream can tell.

The fixtures carry `2**62 + 1` and a negative int64 for exactly this reason.

## Fixtures

Written by pyarrow (`test/fixtures/generate.py`), and so are the expectations:
`ground-truth.edn` is emitted by the same script from the files it just wrote.
A fixture this repo generated itself would test the decoder against its own
misunderstanding of the format, and expectations transcribed by hand would
drift from the fixtures the first time either was regenerated.

The fixture table is deliberately **the same nine rows, four columns and three
batch boundaries** that `org-apache-parquet` uses. Both readers are asserted
against the same reference writer's values, so the two formats' decoded output
is comparable without either repo depending on the other.

```
python3 -m venv .venv && .venv/bin/pip install pyarrow
.venv/bin/python test/fixtures/generate.py
```

CI regenerates them and fails on a diff: the fixtures must stay byte-identical
to what the reference writer produces, or the suite is testing this repo
against its own past misunderstanding rather than against Arrow.

## Tests

```
clojure -M:test                                             # JVM
nbb --classpath "src:test:$(clojure -Spath)" test/run.cljs  # cljs, interpreted
clojure -M:cljs -m cljs.main --target node -m arrow.cljs-runner
clojure -M:lint
```

The ClojureScript runs are not a duplicate of the JVM one: the int64 refusal is
a claim only they can check, and `test/arrow/lake_e2e_test.clj` is JVM-only on
purpose.

`lake_e2e_test` runs the whole chain — catalog → authorization → triple
pattern → buffer range — over a real Arrow file, against `kotobase-lake` as a
test-only dependency. It is the end that can prove the lake's scan seam took a
second format without changing, including the part that is not free: the
identical query that skips two of three row groups on Parquet reads all three
record batches here, and the test asserts that rather than glossing it.

## License

Apache-2.0.
