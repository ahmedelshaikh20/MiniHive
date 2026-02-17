# MiniHive

MiniHive is a lightweight, educational query engine that executes a subset of SQL by translating it into relational algebra (RA), applying optimization rules, and compiling the result into a chain of Luigi MapReduce jobs (Local/HDFS). It also includes a simple I/O cost counter for intermediate results.

## How it works

Pipeline (high-level):
1. Parse SQL query
2. Translate SQL → Relational Algebra AST
3. Apply logical optimization rules
4. Compile RA → Luigi MapReduce task graph
5. Execute in LOCAL or HDFS environment
6. (Optional) compute approximate I/O cost

Core entrypoint: `miniHive.py` 

## Main modules

- **miniHive.py**
  - CLI runner
  - Calls SQL translation, optimization rules, and task compilation
  - Defines the dataset dictionary/schema (TPC-H style tables) 

- **sql2ra.py**
  - Translates a subset of SQL into RA:
    - `SELECT ... FROM ... WHERE ...`
    - AND-combined equality comparisons in WHERE
    - basic table aliases via `RENAME`

- **raopt.py**
  - Relational algebra optimizer rules:
    - Break up conjunctive selections
    - Push down selections
    - Merge selections
    - Introduce joins from selections over cross products
    - Push down projections 

- **ra2mr.py**
  - Compiles RA AST into Luigi MapReduce tasks (LOCAL/HDFS/MOCK)
  - Implements physical operators like Select/Project/Rename/Join
  - Supports optional “chain folding” (combining Select/Project/Rename into one MapReduce step) 

- **costcounter.py**
  - Simple cost model: sums the JSON byte-size of intermediate tuples in local `*.tmp` files

## Supported SQL subset (current)

This project is intentionally minimal. It supports:
- `SELECT *` or selecting explicit columns
- `FROM` with multiple relations (initially translated as cross products)
- `WHERE` with equality comparisons, combined using `AND`
- Table aliases (handled as RA rename)

Not supported (examples):
- Aggregations (`GROUP BY`, `COUNT`, etc.)
- `ORDER BY`, `LIMIT`
- Outer joins
- Complex predicates beyond the implemented subset

(See `sql2ra.py` for translation rules.)

## Running
You’ll need Python packages used by the code:
- `luigi`
- `sqlparse`
- `radb` (relational algebra AST/parser used in the project)




