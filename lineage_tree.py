import pyodbc


# =========================
# CONNECTION
# =========================
def get_connection():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=RakeshReddy\\SQLEXPRESS;"
        "DATABASE=LineageDB;"
        "Trusted_Connection=yes;"
    )

# =========================
# GET OBJECT TYPE
# =========================
def get_object_type(obj):
    obj_upper = obj.upper()

    if obj_upper.startswith("SP") or "PROC" in obj_upper:
        return "Stored Procedure"
    elif "VIEW" in obj_upper:
        return "View"
    else:
        return "Table"


# =========================
# GET FULL DEFINITION
# =========================
def get_dependencies(obj):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT referenced_entity_name
        FROM sys.sql_expression_dependencies
        WHERE OBJECT_NAME(referencing_id) = ?
    """, (obj,))

    rows = cursor.fetchall()
    conn.close()

    return list(set([r[0] for r in rows if r[0] is not None]))


# =========================
# CLEAN LOGIC (REMOVE CREATE LINE)
# =========================
def clean_logic(definition):
    if not definition:
        return "No logic found"

    lines = definition.splitlines()

    # remove first line (CREATE VIEW / CREATE PROCEDURE)
    if len(lines) > 1:
        return " ".join(lines[1:]).strip()

    return definition


# =========================
# GET DEPENDENCIES
# =========================
def get_definition(obj):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT OBJECT_DEFINITION(OBJECT_ID(?))
    """, (obj,))

    row = cursor.fetchone()
    conn.close()

    return row[0] if row and row[0] else "No definition found"


# =========================
# SHOW OBJECT DETAILS
# =========================
def show_object(obj):
    definition = get_definition(obj)
    obj_type = get_object_type(obj)
    uses = get_dependencies(obj)

    print(f"\n{obj}")
    print(f"  ├── Type: {obj_type}")
    print(f"  ├── Logic: {clean_logic(definition)}")
    print(f"  └── Uses:")

    if uses:
        for u in uses:
            print(f"      └── {u}")
    else:
        print("      └── None")


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    print("\n===== SQL LINEAGE REPORT =====")

    objects = [
        "SP_TotalSales",
        "View_Sales"
    ]

    for obj in objects:
        show_object(obj)