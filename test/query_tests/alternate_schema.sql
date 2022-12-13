DROP TABLE IF EXISTS "table2_alt";
CREATE TABLE "table2_alt" (
  "row_number" BIGINT,
  "child" TEXT,
  "parent" TEXT,
  "xyzzy" TEXT,
  "foo" INTEGER,
  "bar" TEXT,
  FOREIGN KEY ("child") REFERENCES "table4"("other_foreign_column"),
  FOREIGN KEY ("foo") REFERENCES "table4"("numeric_foreign_column")
);
CREATE UNIQUE INDEX "table2_alt_child_idx" ON "table2_alt"("child");
CREATE UNIQUE INDEX "table2_alt_row_number_idx" ON "table2_alt"("row_number");

DROP TABLE IF EXISTS "table2_alt_conflict";
CREATE TABLE "table2_alt_conflict" (
  "row_number" BIGINT,
  "child" TEXT,
  "parent" TEXT,
  "xyzzy" TEXT,
  "foo" INTEGER,
  "bar" TEXT
);
CREATE UNIQUE INDEX "table2_alt_conflict_row_number_idx" ON "table2_alt_conflict"("row_number");

DROP VIEW IF EXISTS "table2_alt_view";
CREATE VIEW "table2_alt_view" AS SELECT * FROM "table2_alt" UNION ALL SELECT * FROM "table2_alt_conflict";

INSERT INTO table2_alt ("row_number", "child", "parent", "xyzzy", "foo", "bar")
SELECT "row_number", "child", "parent", "xyzzy", "foo", "bar"
  FROM table2;

INSERT INTO table2_alt_conflict ("row_number", "child", "parent", "xyzzy", "foo", "bar")
SELECT "row_number", "child", "parent", "xyzzy", "foo", "bar"
  FROM table2_conflict;

------------------------------------------------

DROP TABLE IF EXISTS "cell";
CREATE TABLE "cell" (
  "table" TEXT,
  "row" BIGINT,
  "column" TEXT,
  "nullvalue" TEXT,
  "valid" BOOLEAN,
  PRIMARY KEY ("table", "row", "column")
);

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'child',
       CASE WHEN "child_meta" IS NOT NULL
         THEN JSON_EXTRACT("child_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "child_meta" IS NOT NULL
         THEN JSON_EXTRACT("child_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'child',
       CASE WHEN "child_meta" IS NOT NULL
         THEN JSON_EXTRACT("child_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "child_meta" IS NOT NULL
         THEN JSON_EXTRACT("child_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2_conflict";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'parent',
       CASE WHEN "parent_meta" IS NOT NULL
         THEN JSON_EXTRACT("parent_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "parent_meta" IS NOT NULL
         THEN JSON_EXTRACT("parent_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'parent',
       CASE WHEN "parent_meta" IS NOT NULL
         THEN JSON_EXTRACT("parent_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "parent_meta" IS NOT NULL
         THEN JSON_EXTRACT("parent_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2_conflict";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'xyzzy',
       CASE WHEN "xyzzy_meta" IS NOT NULL
         THEN JSON_EXTRACT("xyzzy_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "xyzzy_meta" IS NOT NULL
         THEN JSON_EXTRACT("xyzzy_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'xyzzy',
       CASE WHEN "xyzzy_meta" IS NOT NULL
         THEN JSON_EXTRACT("xyzzy_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "xyzzy_meta" IS NOT NULL
         THEN JSON_EXTRACT("xyzzy_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2_conflict";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'foo',
       CASE WHEN "foo_meta" IS NOT NULL
         THEN JSON_EXTRACT("foo_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "foo_meta" IS NOT NULL
         THEN JSON_EXTRACT("foo_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'foo',
       CASE WHEN "foo_meta" IS NOT NULL
         THEN JSON_EXTRACT("foo_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "foo_meta" IS NOT NULL
         THEN JSON_EXTRACT("foo_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2_conflict";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'bar',
       CASE WHEN "bar_meta" IS NOT NULL
         THEN JSON_EXTRACT("bar_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "bar_meta" IS NOT NULL
         THEN JSON_EXTRACT("bar_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2";

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2_alt',
       row_number AS row,
       'bar',
       CASE WHEN "bar_meta" IS NOT NULL
         THEN JSON_EXTRACT("bar_meta", '$.nulltype')
       ELSE NULL
       END AS nullvalue,
       CASE WHEN "bar_meta" IS NOT NULL
         THEN JSON_EXTRACT("bar_meta", '$.valid')
       ELSE TRUE
       END AS valid
  FROM "table2_conflict";

------------------------------------------------

-- This table is loaded using python:

DROP TABLE IF EXISTS "message";
CREATE TABLE "message" (
  "table" TEXT,
  "row" BIGINT,
  "column" TEXT,
  "level" TEXT,
  "rule" TEXT,
  "message" TEXT,
  PRIMARY KEY ("table", "row", "column", "rule")
);
CREATE INDEX "message_idx" ON "message"("table", "row", "column");
