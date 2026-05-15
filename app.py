from flask import Flask, render_template, request
import pyodbc
import re

app = Flask(__name__)

# -------------------------
# CONNECTION FUNCTION
# -------------------------
def get_connection(server, database):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


# -------------------------
# GET TABLE STRUCTURE
# -------------------------
def get_tables(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_NAME
    """)
    tables = [row[0] for row in cursor.fetchall()]

    table_list = []
    for t in tables:
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME='{t}'
            ORDER BY ORDINAL_POSITION
        """)
        cols = [c[0] for c in cursor.fetchall()]
        table_list.append({"name": t, "columns": cols})

    conn.close()
    return table_list


# -------------------------
# GET VIEWS + PROCEDURES
# -------------------------
def get_objects(server, database):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, type_desc 
        FROM sys.objects
        WHERE type IN ('V','P')
        ORDER BY type_desc, name
    """)
    objects = cursor.fetchall()
    conn.close()

    results = []
    for name, type_desc in objects:
        results.append({"name": name, "type": type_desc})

    return results


# -------------------------
# GET OBJECT DEFINITION
# -------------------------
def get_definition(server, database, obj_name):
    conn = get_connection(server, database)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT definition
        FROM sys.sql_modules
        WHERE object_id = OBJECT_ID(?)
    """, obj_name)

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return row[0]
    return ""


# -------------------------
# FIND USED TABLES/OBJECTS
# -------------------------
def find_used_objects(definition):
    used = []
    tables = re.findall(r'FROM\s+([\w\.]+)', definition, re.IGNORECASE)
    joins = re.findall(r'JOIN\s+([\w\.]+)', definition, re.IGNORECASE)

    all_used = tables + joins

    for t in all_used:
        t = t.replace("dbo.", "")
        if t not in used:
            used.append(t)

    return used


# -------------------------
# FIND COLUMN MAPPING (AS)
# -------------------------
def find_column_mapping(definition):
    mapping = []
    matches = re.findall(r'(\w+)\s+AS\s+(\w+)', definition, re.IGNORECASE)

    for original, alias in matches:
        if original.lower() != alias.lower():
            mapping.append({"original": original, "alias": alias})

    return mapping


# -------------------------
# BUILD LINEAGE OBJECTS
# -------------------------
def build_lineage(server, database):
    objs = get_objects(server, database)
    lineage = []

    for obj in objs:
        name = obj["name"]
        obj_type = obj["type"]

        definition = get_definition(server, database, name)
        used_objects = find_used_objects(definition)
        mapping = find_column_mapping(definition)

        if obj_type == "VIEW":
            ttype = "VIEW"
        else:
            ttype = "STORED PROCEDURE"

        lineage.append({
            "name": name,
            "type": ttype,
            "uses": used_objects,
            "mapping": mapping
        })

    return lineage


# -------------------------
# FINAL LINEAGE CHAIN
# -------------------------
def build_final_lineage(objects):
    lineage_chains = []
    obj_dict = {obj["name"]: obj for obj in objects}

    for obj in objects:
        if obj["type"] == "STORED PROCEDURE":
            if not obj["uses"]:
                continue

            used_obj = obj["uses"][0]

            for mp in obj["mapping"]:
                source_col = mp["original"]
                target_col = mp["alias"]

                chain_parts = [
                    f"{used_obj}.{source_col}",
                    f"{obj['name']}.{target_col}"
                ]

                if used_obj in obj_dict:
                    view_obj = obj_dict[used_obj]

                    if view_obj["uses"]:
                        base_table = view_obj["uses"][0]

                        for vmap in view_obj["mapping"]:
                            if vmap["alias"].lower() == source_col.lower():
                                base_col = vmap["original"]
                                chain_parts.insert(0, f"{base_table}.{base_col}")

                lineage_chains.append(" → ".join(chain_parts))

    return lineage_chains


# -------------------------
# ROUTE
# -------------------------
@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        server1 = request.form.get("server1")
        db1 = request.form.get("database1")

        server2 = request.form.get("server2")
        db2 = request.form.get("database2")

        try:
            # SERVER 1 DATA
            tables1 = get_tables(server1, db1)
            lineage1 = build_lineage(server1, db1)
            final1 = build_final_lineage(lineage1)

            # SERVER 2 DATA
            tables2 = get_tables(server2, db2)
            lineage2 = build_lineage(server2, db2)
            final2 = build_final_lineage(lineage2)

            return render_template(
                "index.html",
                server1=server1, db1=db1, tables1=tables1, lineage1=lineage1, final1=final1,
                server2=server2, db2=db2, tables2=tables2, lineage2=lineage2, final2=final2
            )

        except Exception as e:
            return render_template("index.html", error=str(e))

    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)