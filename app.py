from flask import Flask, render_template, request
import pyodbc
import re

app = Flask(__name__)

# ================= DYNAMIC CONNECTION =================
def get_connection(server, database):
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )

# ================= GET DATABASE NAME =================
def get_database_name(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()
    cursor.execute("SELECT DB_NAME()")
    db = cursor.fetchone()[0]
    conn.close()
    return db

# ================= GET TABLES =================
def get_tables(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
    """)

    tables = [r[0] for r in cursor.fetchall()]
    conn.close()
    return tables

# ================= GET TABLE COLUMNS =================
def get_table_columns(server, database, table):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
    """, (table,))

    cols = [r[0] for r in cursor.fetchall()]
    conn.close()
    return cols

# ================= GET VIEWS =================
def get_views(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
    views = [r[0] for r in cursor.fetchall()]
    conn.close()
    return views

# ================= GET PROCEDURES =================
def get_procedures(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name
        FROM sys.objects
        WHERE type = 'P'
    """)

    procs = [r[0] for r in cursor.fetchall()]
    conn.close()
    return procs

# ================= GET DEFINITION =================
def get_definition(server, database, obj):
    conn = get_connection(server, database)
    cursor = conn.cursor()
    cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", (obj,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row and row[0] else ""

# ================= GET DEPENDENCIES =================
def get_dependencies(server, database, obj):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT referenced_entity_name
        FROM sys.sql_expression_dependencies
        WHERE OBJECT_NAME(referencing_id) = ?
    """, (obj,))

    uses = [r[0] for r in cursor.fetchall() if r[0]]
    conn.close()
    return list(set(uses))

# ================= EXTRACT COLUMN MAPPING =================
def extract_alias_mapping(sql_text):
    if not sql_text:
        return []

    mapping = []
    sql_upper = sql_text.upper()

    # keep only SELECT part
    if "SELECT" in sql_upper:
        sql_text = sql_text[sql_upper.index("SELECT"):]

    # Find "col AS alias"
    pattern = r"(\w+)\s+AS\s+(\w+)"
    matches = re.findall(pattern, sql_text, re.IGNORECASE)

    for original, alias in matches:
        mapping.append({
            "original": original,
            "alias": alias
        })

    return mapping

# ================= BUILD LINEAGE OBJECTS =================
def get_lineage_objects(server, database):
    objects = []

    # Views
    for v in get_views(server, database):
        logic = get_definition(server, database, v)
        deps = get_dependencies(server, database, v)
        mapping = extract_alias_mapping(logic)

        objects.append({
            "name": v,
            "type": "VIEW",
            "uses": deps,
            "mapping": mapping
        })

    # Stored Procedures
    for p in get_procedures(server, database):
        logic = get_definition(server, database, p)
        deps = get_dependencies(server, database, p)
        mapping = extract_alias_mapping(logic)

        objects.append({
            "name": p,
            "type": "STORED PROCEDURE",
            "uses": deps,
            "mapping": mapping
        })

    return objects

# ================= FINAL COLUMN LINEAGE (WITH OBJECT NAMES) =================
def build_final_lineage(objects):
    lineage_chains = []
    obj_dict = {obj["name"]: obj for obj in objects}

    for obj in objects:
        if obj["type"] == "STORED PROCEDURE":

            if not obj["uses"]:
                continue

            used_view = obj["uses"][0]   # example: vw_empdetails

            for mp in obj["mapping"]:
                source_col = mp["original"]   # employeeid
                target_col = mp["alias"]      # EEID

                # start chain with view -> procedure
                chain_parts = [
                    f"{used_view}.{source_col}",
                    f"{obj['name']}.{target_col}"
                ]

                # if SP uses view, trace view mapping back to table
                if used_view in obj_dict:
                    view_obj = obj_dict[used_view]

                    if view_obj["uses"]:
                        base_table = view_obj["uses"][0]  # example: emp

                        for vmap in view_obj["mapping"]:
                            if vmap["alias"].lower() == source_col.lower():
                                base_col = vmap["original"]  # eid
                                chain_parts.insert(0, f"{base_table}.{base_col}")

                lineage_chains.append(" → ".join(chain_parts))

    return lineage_chains

# ================= HOME PAGE =================
@app.route("/", methods=["GET", "POST"])
def home():
    server = ""
    database = ""
    dbname = ""
    structure = {"tables": []}
    lineage_objects = []
    final_lineage = []
    error = ""

    if request.method == "POST":
        server = request.form.get("server")
        database = request.form.get("database")

        try:
            dbname = get_database_name(server, database)

            # Tables with columns
            table_list = []
            for t in get_tables(server, database):
                table_list.append({
                    "name": t,
                    "columns": get_table_columns(server, database, t)
                })

            structure = {"tables": table_list}

            lineage_objects = get_lineage_objects(server, database)
            final_lineage = build_final_lineage(lineage_objects)

        except Exception as e:
            error = str(e)

    return render_template(
        "index.html",
        server=server,
        database=database,
        dbname=dbname,
        structure=structure,
        lineage=lineage_objects,
        final_lineage=final_lineage,
        error=error
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)