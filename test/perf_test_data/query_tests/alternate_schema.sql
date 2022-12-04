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
SELECT 'table2',
       row_number AS row,
       'child',
       JSON_EXTRACT("child_meta", '$.nulltype') AS nullvalue,
       JSON_EXTRACT("child_meta", '$.valid') AS valid
  FROM "table2"
 WHERE "child_meta" IS NOT NULL;

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2',
       row_number AS row,
       'parent',
       JSON_EXTRACT("parent_meta", '$.nulltype') AS nullvalue,
       JSON_EXTRACT("parent_meta", '$.valid') AS valid
  FROM "table2"
 WHERE "parent_meta" IS NOT NULL;

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2',
       row_number AS row,
       'xyzzy',
       JSON_EXTRACT("xyzzy_meta", '$.nulltype') AS nullvalue,
       JSON_EXTRACT("xyzzy_meta", '$.valid') AS valid
  FROM "table2"
 WHERE "xyzzy_meta" IS NOT NULL;

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2',
       row_number AS row,
       'foo',
       JSON_EXTRACT("foo_meta", '$.nulltype') AS nullvalue,
       JSON_EXTRACT("foo_meta", '$.valid') AS valid
  FROM "table2"
 WHERE "foo_meta" IS NOT NULL;

INSERT INTO "cell" ("table", "row", "column", "nullvalue", "valid")
SELECT 'table2',
       row_number AS row,
       'bar',
       JSON_EXTRACT("bar_meta", '$.nulltype') AS nullvalue,
       JSON_EXTRACT("bar_meta", '$.valid') AS valid
  FROM "table2"
 WHERE "bar_meta" IS NOT NULL;

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
