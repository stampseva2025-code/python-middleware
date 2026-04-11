
# A very simple Flask Hello World app for you to get started with...
import os
from flask import Flask, request, jsonify
import mysql.connector
from flask_bcrypt import Bcrypt
from flask_cors import CORS
import json
import decimal
import datetime

app = Flask(__name__)

CORS(app)  # Add this line
bcrypt = Bcrypt(app)


@app.route('/')
def hello_world():
    return 'Hello from Flask!'



# --- SECURITY CONFIG ---
# This is your secret password. Change it to something unique!
SHARED_SECRET_KEY = ""

# --- DATABASE CONFIG ---
db_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': ''
}




# 1. Get All Stamps for Admin Grid
@app.route('/admin/get-all-stamps', methods=['GET'])
def get_all_stamps():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    # 1. Get Parameters from URL
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 100))
    search = request.args.get('search', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    sort_by = request.args.get('sort_by', 'id')
    order = request.args.get('order', 'DESC')

    offset = (page - 1) * limit

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 2. Build Dynamic SQL Query
        base_query = "FROM stamps WHERE 1=1"
        params = []

        if search:
            base_query += " AND (fileName LIKE %s OR Country LIKE %s OR THEME LIKE %s OR Operator LIKE %s)"
            like_val = f"%{search}%"
            params.extend([like_val, like_val, like_val, like_val])

        if date_from:
            base_query += " AND created_at >= %s"
            params.append(f"{date_from} 00:00:00")

        if date_to:
            base_query += " AND created_at <= %s"
            params.append(f"{date_to} 23:59:59")

        # 3. Get Total Count
        cursor.execute(f"SELECT COUNT(*) as total {base_query}", params)
        total_records = cursor.fetchone()['total']

        # 4. Get Paginated Data
        allowed_cols = ['id', 'fileName', 'Country', 'Year', 'created_at']
        final_sort = sort_by if sort_by in allowed_cols else 'id'
        final_order = 'DESC' if order.upper() == 'DESC' else 'ASC'

        full_query = f"SELECT * {base_query} ORDER BY {final_sort} {final_order} LIMIT %s OFFSET %s"

        # Prepare params for the data fetch (Filters + Limit/Offset)
        data_params = list(params)
        data_params.extend([limit, offset])

        cursor.execute(full_query, data_params)
        data = cursor.fetchall()

        return jsonify({
            "success": True,
            "data": data,
            "total": total_records,
            "page": page,
            "limit": limit
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals(): cursor.close(); conn.close()


@app.route('/api/cloud/receive-sync-chunk', methods=['POST'])
def receive_sync_chunk():
    # 1. SECURITY CATCH: Match existing auth style
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json or {}
    table = data.get('table')
    columns = data.get('columns')
    values = data.get('values')

    if not table or not columns or not values:
        return jsonify({"success": False, "error": "Missing table, columns, or values in payload"}), 400

    try:
        # 2. Connect using existing db_config style
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # =========================================================
        # 🚦 THE FIX: Setup Session Variables
        # =========================================================
        cursor.execute("SET time_zone = '+00:00'")
        # Tell MySQL to temporarily ignore parent/child relationships
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        # =========================================================

        # 3. Build the Upsert (ON DUPLICATE KEY UPDATE) Query dynamically
        columns_sql = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        # Don't update the primary keys if they exist
        update_cols = [f"`{col}`=VALUES(`{col}`)" for col in columns if col not in ('id', 'stamp_id')]
        update_string = ", ".join(update_cols)

        # Handle the edge case where a table might only have primary keys (like a pure join table)
        if not update_cols:
            sql = f"""
                INSERT IGNORE INTO `{table}` ({columns_sql})
                VALUES ({placeholders})
            """
        else:
            sql = f"""
                INSERT INTO `{table}` ({columns_sql})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_string}
            """

        # 4. Format the values as tuples for MySQL
        tuple_values = [tuple(v) for v in values]

        # 5. Execute the batch insert
        cursor.executemany(sql, tuple_values)

        # =========================================================
        # 🚦 TURN SECURITY BACK ON BEFORE COMMITTING
        # =========================================================
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        # =========================================================

        conn.commit()

        return jsonify({
            "success": True,
            "message": f"Successfully processed chunk of {len(values)} records for {table}."
        })

    except Exception as e:
        # If something goes wrong, rollback so we don't corrupt the database
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        # Match existing compact cleanup style
        if 'conn' in locals(): cursor.close(); conn.close()


@app.route('/admin/dashboard-stats', methods=['GET'])
def get_dashboard_stats():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # A. Total Cataloged Count & Total Processed (Rows + Extra Copies)
        # We use NULLIF and CAST to safely convert the varchar to an integer.
        cursor.execute("""
            SELECT
                COUNT(*) as total_rows,
                COALESCE(SUM(CAST(NULLIF(extra_copies, '') AS UNSIGNED)), 0) as total_extras
            FROM stamps
        """)
        counts = cursor.fetchone()

        total_count = counts['total_rows']
        total_processed = total_count + int(counts['total_extras'])

        # B. Value Distribution (Country)
        cursor.execute("SELECT Country, COUNT(*) as count FROM stamps WHERE Country IS NOT NULL GROUP BY Country ORDER BY count DESC LIMIT 10")
        countries = cursor.fetchall()

        # C. Theme Heatmap (Top Themes)
        cursor.execute("SELECT THEME, COUNT(*) as count FROM stamps WHERE THEME IS NOT NULL GROUP BY THEME ORDER BY count DESC LIMIT 10")
        themes = cursor.fetchall()

        # D. Recent Activity (Last 5)
        cursor.execute("SELECT fileName, Operator, THEME, Year, created_at FROM stamps ORDER BY created_at DESC LIMIT 5")
        recent_activity = cursor.fetchall()

        cursor.execute("""
            SELECT
                sh.sheet_name as folder,
                COUNT(s.id) as total_stamps,
                COUNT(CASE WHEN s.ai_raw IS NOT NULL AND s.ai_raw != '' THEN 1 END) as ai_count,
                MAX(s.Operator) as last_op,
                MAX(s.created_at) as last_update,
                MAX(sh.country) as country
            FROM sheets sh
            LEFT JOIN stamps s ON sh.sheet_name = s.folder
            GROUP BY sh.sheet_name
            ORDER BY sh.sheet_name ASC
        """)
        sheets = cursor.fetchall()

        return jsonify({
            "success": True,
            "total": total_count,
            "total_processed": total_processed,  # <-- Added your new metric here!
            "distributions": {
                "countries": countries,
                "themes": themes
            },
            "activity": recent_activity,
            "sheets": sheets
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals(): cursor.close(); conn.close()

# 2. Update Stamp Record (Edit)
@app.route('/admin/update-stamp', methods=['POST'])
def update_stamp():
    client_key = request.headers.get("X-API-KEY")
    if client_key != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        data = request.json
        stamp_id = data.get('id')
        updates = data.get('updates') # Dictionary of field:value pairs

        if not stamp_id or not updates:
            return jsonify({"success": False, "error": "Missing data"}), 400

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Build dynamic SQL query for efficiency
        for field, value in updates.items():
            sql = f"UPDATE stamps SET {field} = %s WHERE id = %s"
            cursor.execute(sql, (value, stamp_id))

        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True, "message": "Record updated successfully"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# 3. Delete Stamp Record
@app.route('/admin/delete-stamp', methods=['POST'])
def delete_admin_stamp():
    client_key = request.headers.get("X-API-KEY")
    if client_key != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        data = request.json
        stamp_id = data.get('id')

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM stamps WHERE id = %s", (stamp_id,))

        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True, "message": "Record deleted"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/get-stamps-by-ids-app', methods=['POST'])
def get_stamps_by_ids_app():
    # 1. Security Check
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    # 2. Extract the IDs from the incoming request
    data = request.json
    if not data or 'ids' not in data or not data['ids']:
        return jsonify({"error": "No IDs provided"}), 400

    # Ensure all IDs are integers for safety
    try:
        id_list = [int(i) for i in data['ids']]
    except ValueError:
        return jsonify({"error": "Invalid ID format"}), 400

    conn = None
    cursor = None

    try:
        # 3. Database Connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 4. Create the secure SQL Query
        # This creates a string of placeholders like: %s, %s, %s
        placeholders = ', '.join(['%s'] * len(id_list))
        sql = f"SELECT * FROM stamps WHERE id IN ({placeholders})"

        # Execute securely by passing the list as a tuple
        cursor.execute(sql, tuple(id_list))
        db_results = cursor.fetchall()

        final_candidates = []

        for row in db_results:
            formatted_row = {
                "id": row.get("id"),
                "fileName": row.get("fileName"),
                "drive_url": row.get("drive_url"),
                "image_vector": [],  # Added as requested
                "score": 33,         # Added as requested
                "votes": 1,          # Added as requested
                "stamp_info": row    # This dumps ALL the columns from the database into this nested object!
            }

            final_candidates.append(formatted_row)

        # 5. Return result as JSON
        return jsonify({"success": True, "data": final_candidates})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        # 6. Close Connection
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-stamps-by-ids', methods=['POST'])
def get_stamps_by_ids():
    # 1. Security Check
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    # 2. Extract the IDs from the incoming request
    data = request.json
    if not data or 'ids' not in data or not data['ids']:
        return jsonify({"error": "No IDs provided"}), 400

    # Ensure all IDs are integers for safety
    try:
        id_list = [int(i) for i in data['ids']]
    except ValueError:
        return jsonify({"error": "Invalid ID format"}), 400

    conn = None
    cursor = None

    try:
        # 3. Database Connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 4. Create the secure SQL Query
        # This creates a string of placeholders like: %s, %s, %s
        placeholders = ', '.join(['%s'] * len(id_list))
        # sql = f"SELECT id, fileName, drive_url FROM stamps WHERE id IN ({placeholders})"
        sql = f"SELECT * FROM stamps WHERE id IN ({placeholders})"

        # Execute securely by passing the list as a tuple
        cursor.execute(sql, tuple(id_list))
        db_results = cursor.fetchall()

        final_candidates = []

        for row in db_results:
            formatted_row = {
                "id": row.get("id"),
                "fileName": row.get("fileName"),
                "drive_url": row.get("drive_url"),
                "image_vector": [],  # Added as requested
                "score": 33,         # Added as requested
                "votes": 1,          # Added as requested
                "stamp_info": row    # This dumps ALL the columns from the database into this nested object!
            }

            final_candidates.append(formatted_row)

        # 5. Return result as JSON
        return jsonify({"success": True, "data": final_candidates})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        # 6. Close Connection
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-all-vectors', methods=['GET'])
def get_all_vectors():
    # 1. Security Check
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    conn = None
    cursor = None

    try:
        # 2. Database Connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 3. SQL Query: Get ALL stamps that HAVE a valid vector
        # We only need 'id' and 'image_vector' to keep the download fast and lightweight
        sql = "SELECT id, image_vector, Country FROM stamps WHERE image_vector IS NOT NULL AND CHAR_LENGTH(image_vector) > 10"

        cursor.execute(sql)
        result = cursor.fetchall()

        # 4. Return result as JSON
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        # 5. Close Connection
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-stamps-batch', methods=['GET'])
def get_stamps_batch():
    # 1. Security Check
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    conn = None
    cursor = None

    try:
        # 2. Database Connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 3. SQL Query: Get 100 stamps that DO NOT have a vector yet
        # We fetch 'imagePath' (for the file location) and 'fileName' (to identify it)
        sql = "SELECT * FROM stamps WHERE image_vector IS NULL OR CHAR_LENGTH(image_vector) < 10"

        cursor.execute(sql)
        result = cursor.fetchall()

        # 4. Return result as JSON
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        # 5. Close Connection
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-users', methods=['GET'])
def get_users():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403
    try:
        conn = mysql.connector.connect(**db_config)
        # Using dictionary=True so we get {'id': 1, 'name': 'navin'}
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id, name FROM users")
        result = cursor.fetchall()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/sync-sequence', methods=['GET'])
def sync_sequence():
    # 2. Get Folder Names (Hardcoded as per your example or use request.args.get)
    prev_folder = 'A10002a'
    next_folder = 'A10002b'

    if not prev_folder or not next_folder:
        return jsonify({"error": "Missing 'prev' or 'next' parameters"}), 400

    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # STEP A: Get the count of the previous folder (The Offset)
        cursor.execute("SELECT COUNT(*) as total FROM stamps WHERE folder = %s", (prev_folder,))
        offset_result = cursor.fetchone()
        offset = offset_result['total'] if offset_result else 0

        # STEP B: Get records from the next folder to update
        cursor.execute("SELECT id, fileName FROM stamps WHERE folder = %s ORDER BY id ASC", (next_folder,))
        records = cursor.fetchall()

        if not records:
            return jsonify({
                "message": f"No records found in {next_folder}",
                "prev_count": offset
            }), 200

        # --- STEP C: PASS 1 - Temporary Rename ---
        # We add '_TEMP' to the end to bypass the Unique Constraint
        for record in records:
            temp_name = f"{record['fileName']}_TEMP_{record['id']}"
            cursor.execute("UPDATE stamps SET fileName = %s WHERE id = %s", (temp_name, record['id']))

        # --- STEP D: PASS 2 - Final Sequential Rename ---
        updated_count = 0
        for i, record in enumerate(records, start=1):
            db_id = record['id']

            # MATH: Previous Count + Current Position
            new_number_value = offset + i
            new_number_str = str(new_number_value).zfill(2)

            # Construct the final fileName (e.g., A10002b-26)
            new_fileName = f"{next_folder}-{new_number_str}"

            # Execute Final Update
            update_sql = "UPDATE stamps SET fileName = %s WHERE id = %s"
            cursor.execute(update_sql, (new_fileName, db_id))
            updated_count += 1

        conn.commit()

        return jsonify({
            "status": "success",
            "prev_folder": prev_folder,
            "next_folder": next_folder,
            "offset_used": offset,
            "records_updated": updated_count
        })

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


@app.route('/api/login-verify', methods=['POST'])
def login_verify():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    user_id = data.get('user_id')
    typed_password = data.get('password')

    if not user_id or not typed_password:
        return jsonify({"success": False, "message": "Missing credentials"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT id, name, password, role FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()

        # Check if user exists and if they actually have a password set
        if user and user['password']:
            if bcrypt.check_password_hash(user['password'], typed_password):
                return jsonify({
                    "success": True,
                    "user": {"id": user['id'], "name": user['name'], "role": user['role']}
                })

        return jsonify({"success": False, "message": "Invalid Login"}), 401
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


@app.route('/api/admin-set-password', methods=['POST'])
def set_password():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    target_id = data.get('target_user_id')
    new_plain_password = data.get('new_password')

    if not target_id or not new_plain_password:
        return jsonify({"success": False, "message": "Missing data"}), 400

    # This creates the secure encrypted hash
    hashed_password = bcrypt.generate_password_hash(new_plain_password).decode('utf-8')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET password = %s WHERE id = %s", (hashed_password, target_id))
        conn.commit()

        # Check if any row was actually updated
        if cursor.rowcount == 0:
            return jsonify({"success": False, "message": "User not found"}), 404

        return jsonify({"success": True, "message": "Password encrypted and saved!"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/save-upload', methods=['POST'])
def save_upload():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        sql = """INSERT INTO uploads
                 (fileName, fullPath, uploadedBy, uploadDate, uploadTime, isCropped)
                 VALUES (%s, %s, %s, %s, %s, %s)"""

        values = (
            data.get('fileName'),
            data.get('fullPath'),
            data.get('uploadedBy'),
            data.get('date'),
            data.get('time'),
            data.get('isCropped', False)
        )

        cursor.execute(sql, values)
        conn.commit()
        return jsonify({"success": True, "message": "Metadata saved to cloud"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/check-sheet', methods=['POST'])
def check_sheet():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    sheet_name = data.get('fileName')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        # Check if this filename exists in your sheets table
        cursor.execute("SELECT id FROM sheets WHERE sheet_name = %s", (sheet_name,))
        exists = cursor.fetchone()

        if exists:
            return jsonify({"success": True, "exists": True})
        else:
            return jsonify({"success": True, "exists": False})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()

@app.route('/api/get-all-sheets', methods=['GET'])
def get_all_sheets():
    # Security Check
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True) # returns data as a list of dictionaries

        # Fetch all sheets, newest first
        query = "SELECT * FROM sheets ORDER BY created_at DESC"
        cursor.execute(query)

        all_sheets = cursor.fetchall()

        return jsonify({
            "success": True,
            "count": len(all_sheets),
            "data": all_sheets
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-stamp-details', methods=['POST'])
def get_stamp_details_online():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    file_name = data.get('fileName')

    if not file_name:
        return jsonify({"success": False, "error": "No filename provided"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Query to find the stamp by its unique cropped filename
        # Ensure the column name matches your database (e.g., fileName)
        query = "SELECT * FROM stamps WHERE fileName = %s LIMIT 1"
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()

        if result:
            return jsonify({
                "success": True,
                "exists": True,
                "metadata": result
            })
        else:
            return jsonify({
                "success": True,
                "exists": False
            })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/save-sheet', methods=['POST'])
def save_sheet():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    raw_name = data.get('fileName')
    sheet_name = os.path.splitext(raw_name)[0] if raw_name else None
    new_url = data.get('drive_url')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(buffered=True)

        # 1. Check if sheet_name already exists
        check_sql = "SELECT id, original_image_url FROM sheets WHERE sheet_name = %s"
        cursor.execute(check_sql, (sheet_name,))
        existing_record = cursor.fetchone()

        if existing_record:

            # Inside the "if existing_record" block
            cursor.execute("SELECT v5_url FROM sheets WHERE sheet_name = %s", (sheet_name,))
            discarded_url = cursor.fetchone()[0] # The URL being pushed out of the 5 slots

            # --- VERSIONING LOGIC (The Waterfall) ---
            # We shift v4->v5, v3->v4, v2->v3, v1->v2, and current->v1
            update_sql = """
                UPDATE sheets
                SET
                    v5_url = v4_url,
                    v4_url = v3_url,
                    v3_url = v2_url,
                    v2_url = v1_url,
                    v1_url = original_image_url,
                    original_image_url = %s,
                    country = %s,
                    uploadDate = %s,
                    uploadTime = %s,
                    uploadedBy = %s,
                    status = 'uncropped'
                WHERE sheet_name = %s
            """
            cursor.execute(update_sql, (
                new_url,
                data.get('country'),
                data.get('date'),
                data.get('time'),
                data.get('uploadedBy'),
                sheet_name
            ))
            conn.commit()
            return jsonify({"success": True, "discardedUrl": discarded_url, "message": f"Sheet '{sheet_name}' updated. Previous version moved to v1."})


        # 2. If it doesn't exist, proceed to fresh INSERT
        sql = """INSERT INTO sheets
                 (sheet_name, original_image_url, uploadedBy, uploadDate, uploadTime,
                  status, project_prefix, country, issue_year, description, source)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        values = (
            sheet_name,
            new_url,
            data.get('uploadedBy'),
            data.get('date'),
            data.get('time'),
            'uncropped',
            data.get('project_prefix', 'GENERAL'),
            data.get('country'), '0000', 'Cloud Uploaded Sheet', 'Electron App'
        )

        cursor.execute(sql, values)
        conn.commit()
        return jsonify({"success": True, "message": "New sheet logged to cloud"})

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/api/get-sheet-history', methods=['GET'])
def get_history():
    file_name = request.args.get('fileName').split('.')[0]
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT v1_url, v2_url, v3_url, v4_url, v5_url FROM sheets WHERE sheet_name = %s", (file_name,))
    row = cursor.fetchone()
    conn.close()
    return jsonify({"success": True, "history": row})

@app.route('/api/rollback-sheet', methods=['POST'])
def rollback_sheet():
    if request.headers.get('X-API-KEY') != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    data = request.json
    raw_name = data.get('fileName')
    sheet_name = os.path.splitext(raw_name)[0] if raw_name else None
    idx = data.get('targetVersion') # The version number clicked (1-5)

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1. Fetch the target URL to move to live
        cursor.execute(f"SELECT v{idx}_url FROM sheets WHERE sheet_name = %s", (sheet_name,))
        row = cursor.fetchone()

        # Check for None or Empty String
        if not row or not row[f'v{idx}_url'] or row[f'v{idx}_url'] == "":
            return jsonify({"success": False, "error": "Target version is empty"}), 404

        target_url = row[f'v{idx}_url']

        # 2. DYNAMIC SHIFT (Pulling forward)
        # Example if idx is 2:
        # original = v2, v2 = v3, v3 = v4, v4 = v5, v5 = ""
        set_clauses = [f"original_image_url = %s"]

        # Shift forward only the columns after the target index
        for i in range(idx, 5):
            set_clauses.append(f"v{i}_url = v{i+1}_url")

        # The last version always becomes an empty string to clear the slot
        set_clauses.append("v5_url = ''")
        set_clauses.append("status = 'uncropped'")

        cursor.execute("SELECT original_image_url FROM sheets WHERE sheet_name = %s", (sheet_name,))
        bad_live_url = cursor.fetchone()['original_image_url']

        sql = f"UPDATE sheets SET {', '.join(set_clauses)} WHERE sheet_name = %s"

        cursor.execute(sql, (target_url, sheet_name))
        conn.commit()

        return jsonify({"success": True, "discardedUrl": bad_live_url, "message": f"Restored v{idx}. History shifted forward."})

    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/cloud-save', methods=['POST'])
def cloud_save():
    client_key = request.headers.get("X-API-KEY")
    if client_key != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        data = request.json
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # STEP 1: Insert into 'stamps' with ALL independent fields (Now including fingerprint_whash)
        stamp_sql = """INSERT INTO stamps
                  (sheet_id, fileName, folder, imagePath, drive_url, drive_folder_id,
                   Country, THEME, Year, Color, Denomination, extra_copies, initials,
                   estimated_value, History, Description, historical_context,
                   curator_fun_fact, design_symbolism, narrative_script,
                   Remarks, ai_raw, fingerprint_phash, fingerprint_dhash, fingerprint_whash, Operator, image_vector)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        stamp_values = (
            data.get('sheet_id'), data.get('fileName'), data.get('folder'),
            data.get('imagePath'), data.get('drive_url'), data.get('drive_folder_id'),
            data.get('Country'), data.get('THEME'),
            data.get('Year'), data.get('Color'), data.get('Denomination'),
            data.get('extra_copies'), data.get('initials'), data.get('estimated_value'),
            data.get('History'), data.get('Description'), data.get('historical_context'),
            data.get('curator_fun_fact'), data.get('design_symbolism'),
            data.get('narrative_script'), data.get('Remarks'), data.get('ai_raw'),
            data.get('fingerprint_phash'), data.get('fingerprint_dhash'),
            data.get('fingerprint_whash'), data.get('Operator'), data.get('image_vector') # Added whash here
        )

        cursor.execute(stamp_sql, stamp_values)
        new_stamp_id = cursor.lastrowid

        # STEP 2: Insert into 'stamp_images'
        image_sql = """INSERT INTO stamp_images
                       (stamp_id, image_url, image_hash, has_cancellation, is_primary)
                       VALUES (%s, %s, %s, %s, %s)"""

        image_values = (
            new_stamp_id,
            data.get('drive_url'),
            data.get('fingerprint_phash'),
            0, 1
        )
        cursor.execute(image_sql, image_values)

        # STEP 3: Split comma-separated tags and insert one-by-one into 'stamp_tag'
        theme_tags_str = data.get('theme_tags')
        if theme_tags_str:
            tags_list = [t.strip() for t in theme_tags_str.split(',') if t.strip()]
            tag_sql = "INSERT INTO stamp_tags (stamp_id, tag) VALUES (%s, %s)"
            for tag in tags_list:
                cursor.execute(tag_sql, (new_stamp_id, tag))

        conn.commit()
        return jsonify({"success": True, "stamp_id": new_stamp_id}), 200

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
@app.route('/get-or-create-sheet', methods=['POST'])
def get_or_create_sheet():
    # Security Check
    client_key = request.headers.get("X-API-KEY")
    if client_key != SHARED_SECRET_KEY:
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    try:
        data = request.json
        raw_name = data.get('sheet_name')
        sheet_name = os.path.splitext(raw_name)[0] if raw_name else None


        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1. Check if sheet exists
        cursor.execute("SELECT id FROM sheets WHERE sheet_name = %s", (sheet_name,))
        result = cursor.fetchone()

        if result:
            sheet_id = result['id']
        else:
            # 2. Create new sheet with dummy info
            sql = """INSERT INTO sheets
                     (sheet_name, country, issue_year, description, source)
                     VALUES (%s, %s, %s, %s, %s)"""
            # Dummy info used for missing fields
            values = (sheet_name, "Unknown", "0000", "Auto-generated sheet record", "Electron App")
            cursor.execute(sql, values)
            conn.commit()
            sheet_id = cursor.lastrowid

        cursor.close()
        conn.close()
        return jsonify({"success": True, "sheet_id": sheet_id})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# --- ENDPOINT 1: SEARCH SHEETS ---
@app.route('/api/search-sheets', methods=['GET'])
def search_sheets():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1. Grab 'name' and 'country' from the URL parameters
        name = request.args.get('name', '')
        country = request.args.get('country', '')

        # 2. Base Query (Joins sheets and stamps to get the count)
        sql = """
            SELECT sh.*, COUNT(s.id) as stamp_count
            FROM sheets sh
            LEFT JOIN stamps s ON sh.id = s.sheet_id
            WHERE sh.sheet_name LIKE %s
        """
        params = [f"%{name}%"]

        # 3. If the user typed a country, filter by the new country column
        if country:
            sql += " AND sh.country LIKE %s"
            params.append(f"%{country}%")

        # 4. Group and sort
        sql += " GROUP BY sh.id ORDER BY sh.created_at DESC"

        cursor.execute(sql, params)
        return jsonify({"success": True, "data": cursor.fetchall()})

    except Exception as e:
        print(f"Error searching sheets: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

@app.route('/api/get-sheet-stamps/<int:sheet_id>', methods=['GET'])
def get_sheet_stamps(sheet_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 1. Capture every possible filter from the request
        f = {
            'country': request.args.get('country'),
            'theme': request.args.get('theme'),
            'color': request.args.get('color'),
            'year': request.args.get('year'),
            'initials': request.args.get('initials'),
            'denom': request.args.get('denom'),
            'cancelled': request.args.get('cancelled'), # '1' or '0'
            'missing_ai': request.args.get('missing_ai') # 'true' or 'false'
        }

        # 2. Base SQL Query
        # UPDATED: Added s.drive_url and ensured i.image_url points to the cloud link
        sql = """
            SELECT s.*, s.drive_url, i.image_url, i.has_cancellation
            FROM stamps s
            LEFT JOIN stamp_images i ON s.id = i.stamp_id AND i.is_primary = 1
            WHERE s.sheet_id = %s
        """
        params = [sheet_id]

        # 3. Dynamic Filter Building
        if f['country']:
            sql += " AND s.Country LIKE %s"; params.append(f"%{f['country']}%")
        if f['theme']:
            sql += " AND s.THEME LIKE %s"; params.append(f"%{f['theme']}%")
        if f['color']:
            sql += " AND s.Color LIKE %s"; params.append(f"%{f['color']}%")
        if f['year']:
            sql += " AND s.Year LIKE %s"; params.append(f"%{f['year']}%")
        if f['initials']:
            sql += " AND s.fileName LIKE %s"; params.append(f"{f['initials']}%")
        if f['denom']:
            sql += " AND s.Denomination LIKE %s"; params.append(f"%{f['denom']}%")

        # Advanced Technical Filters
        if f['cancelled'] in ['0', '1']:
            sql += " AND i.has_cancellation = %s"; params.append(int(f['cancelled']))

        if f['missing_ai'] == 'true':
            sql += " AND (s.narrative_script IS NULL OR s.narrative_script = '' OR s.historical_context IS NULL)"

        # 4. Final Order and Execution
        sql += " ORDER BY s.fileName ASC"
        cursor.execute(sql, params)

        return jsonify({"success": True, "data": cursor.fetchall()})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/admin/custom-query', methods=['POST'])
def custom_query():
    try:
        # 1. Connect using your existing style
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 2. Extract Data
        data = request.json
        conditions = data.get('conditions', [])
        page = int(data.get('page', 1))
        limit = int(data.get('limit', 50))
        offset = (page - 1) * limit

        # 3. Base SQL Query
        sql = "SELECT * FROM stamps"
        params = []

        # 4. Safely build the WHERE clause dynamically
        if conditions:
            where_clauses = []

            # List of columns we allow querying (Prevents SQL Injection)
            allowed_cols = [
                "id", "sheet_id", "fileName", "folder", "imagePath", "Country",
                "THEME", "Year", "Color", "Denomination", "extra_copies", "initials",
                "estimated_value", "History", "Description", "historical_context",
                "curator_fun_fact", "design_symbolism", "narrative_script", "Remarks",
                "ai_raw", "fingerprint_phash", "fingerprint_dhash", "fingerprint_whash",
                "Operator", "created_at", "drive_url", "local_path", "drive_folder_id",
                "image_vector"
            ]
            allowed_ops = ["=", "LIKE", "!=", ">", "<"]

            for cond in conditions:
                col = cond.get('column')
                op = cond.get('operator')
                val = cond.get('value')

                # Security Validation: Only append if it perfectly matches allowed lists
                if col in allowed_cols and op in allowed_ops and val:
                    if op == "LIKE":
                        where_clauses.append(f"`{col}` LIKE %s")
                        params.append(f"%{val}%") # Wrap in wildcards for LIKE
                    else:
                        where_clauses.append(f"`{col}` {op} %s")
                        params.append(val)

            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

        # 5. Add pagination / limits
        sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        # 6. Execute query
        cursor.execute(sql, params)
        results = cursor.fetchall()

        # 7. Sanitize dates/decimals for JSON output
        import datetime, decimal
        for row in results:
            for key, value in row.items():
                if isinstance(value, (datetime.date, datetime.datetime)):
                    row[key] = value.isoformat()
                elif isinstance(value, decimal.Decimal):
                    row[key] = float(value)

        return jsonify({"success": True, "data": results})

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        # 8. Clean up connections (Using safe checks in case connection failed early)
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn is not None:
            conn.close()

# Keep your loadSheets and get_sheet_stamps (the gallery one) as they are.
# Just add/update this one for the Admin Audit Detail:

@app.route('/api/get-stamp-full-detail/<int:stamp_id>', methods=['GET'])
def get_stamp_full_detail(stamp_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Pulls everything from Stamps + related Sheet + related Image
        sql = """
            SELECT
                s.*,
                sh.sheet_name, sh.created_at as sheet_created,
                i.image_url, i.image_hash, i.has_cancellation, i.is_primary
            FROM stamps s
            LEFT JOIN sheets sh ON s.sheet_id = sh.id
            LEFT JOIN stamp_images i ON s.id = i.stamp_id
            WHERE s.id = %s
        """
        cursor.execute(sql, (stamp_id,))
        result = cursor.fetchone()

        # --- ADDED: Fetch Tags from stamp_tags table ---
        if result:
            tag_sql = "SELECT tag FROM stamp_tags WHERE stamp_id = %s"
            cursor.execute(tag_sql, (stamp_id,))
            tags_records = cursor.fetchall()
            # Extract just the tag strings into a clean list
            result['tags'] = [r['tag'] for r in tags_records]

        return jsonify({"success": True, "data": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/update-stamp-full/<int:stamp_id>', methods=['POST'])
def update_stamp_full(stamp_id):
    try:
        data = request.json
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Matched exactly to your 'desc stamps' output
        sql = """
            UPDATE stamps SET
                fileName = %s,
                Country = %s,
                THEME = %s,
                Year = %s,
                Color = %s,
                Denomination = %s,
                extra_copies = %s,
                initials = %s,
                estimated_value = %s,
                History = %s,
                Description = %s,
                historical_context = %s,
                curator_fun_fact = %s,
                design_symbolism = %s,
                narrative_script = %s,
                Remarks = %s,
                Operator = %s
            WHERE id = %s
        """

        values = (
            data.get('fileName'),
            data.get('Country'),
            data.get('THEME'),
            data.get('Year'),
            data.get('Color'),
            data.get('Denomination'),
            data.get('extra_copies'),  # Synced from collection_copies
            data.get('initials'),      # New field from your schema
            data.get('estimated_value'),
            data.get('History'),
            data.get('Description'),
            data.get('historical_context'),
            data.get('curator_fun_fact'),
            data.get('design_symbolism'),
            data.get('narrative_script'),
            data.get('Remarks'),       # New field from your schema
            data.get('Operator'),
            stamp_id
        )

        cursor.execute(sql, values)
        conn.commit()
        return jsonify({"success": True, "message": "Record updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/get-all-hashes', methods=['GET'])
def get_all_hashes():
    # Only allow your secret key for security
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Updated SQL to fetch both p-hash and d-hash
        cursor.execute("SELECT fileName, fingerprint_phash, fingerprint_dhash FROM stamps")
        records = cursor.fetchall()

        # Format as a list of dictionaries including both fingerprints
        # 'hash' is kept for backward compatibility, 'phash' and 'dhash' added for clarity
        hash_list = []
        for r in records:
            if r[1]: # Only include if at least phash exists
                hash_list.append({
                    "fileName": r[0],
                    "hash": r[1],        # Original key
                    "phash": r[1],       # Explicit p-hash
                    "dhash": r[2]        # New d-hash column
                })

        return jsonify({"success": True, "hashes": hash_list})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/repair-cloud-row', methods=['POST'])
def repair_cloud_row():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    try:
        data = request.json
        row_id = data.get('id')
        p_in = data.get('phash')
        d_in = data.get('dhash')
        w_in = data.get('whash')

        if not all([row_id, p_in, d_in, w_in]):
            return jsonify({"success": False, "error": "Missing data"}), 400

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update the specific row
        sql = """
            UPDATE stamps
            SET fingerprint_phash = %s,
                fingerprint_dhash = %s,
                fingerprint_whash = %s
            WHERE id = %s
        """
        cursor.execute(sql, (p_in, d_in, w_in, row_id))
        conn.commit()

        return jsonify({"success": True, "message": f"Row {row_id} updated"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/get-broken-stamps', methods=['GET'])
def get_broken_stamps():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        # Use your exact query
        query = "SELECT id, fileName, drive_url FROM stamps WHERE (fingerprint_phash IS NULL OR fingerprint_phash IN ('', 'NO_PHASH')) OR (fingerprint_dhash IS NULL OR fingerprint_dhash IN ('', 'NO_DHASH', 'NO_PHASH')) OR (fingerprint_whash IS NULL OR fingerprint_whash IN ('', 'NO_WHASH', 'NO_PHASH'));"
        cursor.execute(query)
        rows = cursor.fetchall()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

@app.route('/get-pending-audits', methods=['GET'])
def get_pending_audits():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    try:
        # We fetch records that are UNRESOLVED and have a valid target image
        # We sort by checked_at DESC so you see the newest finds first
        query = """
            SELECT
                stamp_id,
                original_id,
                status,
                dist_p,
                dist_d,
                dist_w,
                target_fileName,
                target_drive_url,
                original_fileName,
                original_drive_url,
                checked_at
            FROM duplicate_audit
            WHERE user_resolution = 'UNRESOLVED'
              AND status IN ('DUPLICATE', 'UNCERTAIN')
            ORDER BY checked_at DESC
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Convert timestamps to strings so JSON can handle them
        for row in results:
            if row['checked_at']:
                row['checked_at'] = row['checked_at'].strftime('%Y-%m-%d %H:%M:%S')

        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/resolve-duplicate', methods=['POST'])
def resolve_duplicate():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    stamp_id = data.get('stamp_id')
    action = data.get('action')

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    try:
        conn.start_transaction()

        # Fetch Drive URL for storage cleanup
        cursor.execute("SELECT drive_url FROM stamps WHERE id = %s", (stamp_id,))
        record_info = cursor.fetchone()

        if action == 'DELETE_DATA':

            # Cascading delete to maintain DB integrity
            cursor.execute("DELETE FROM stamp_tags WHERE stamp_id = %s", (stamp_id,))
            cursor.execute("DELETE FROM stamp_images WHERE stamp_id = %s", (stamp_id,))
            cursor.execute("DELETE FROM stamps WHERE id = %s", (stamp_id,))

            # Update audit status
            cursor.execute("UPDATE duplicate_audit SET user_resolution = 'DATA_DELETED' WHERE stamp_id = %s", (stamp_id,))

        elif action == 'DELETE_CROP':
            # Path 2: Delete ONLY the image from Google Drive storage
            if record_info and record_info['drive_url']:
                delete_file_from_drive(record_info['drive_url'])

            cursor.execute("UPDATE duplicate_audit SET user_resolution = 'CROP_FILE_DELETED' WHERE stamp_id = %s", (stamp_id,))

        elif action == 'DELETE_DUPLICATE_ENTRY':
            # Path 3: Only remove the audit record from the review list
            cursor.execute("DELETE FROM duplicate_audit WHERE stamp_id = %s", (stamp_id,))

        conn.commit()
        return jsonify({"success": True, "message": f"Action {action} completed successfully."})

    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/audit-next-stamp', methods=['POST'])
def audit_next_stamp():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    try:
        # 1. Pick the oldest un-audited stamp
        cursor.execute("""
            SELECT id, fileName, drive_url, fingerprint_phash, fingerprint_dhash, fingerprint_whash
            FROM stamps
            WHERE id NOT IN (SELECT stamp_id FROM duplicate_audit)
            ORDER BY id ASC LIMIT 1
        """)
        target = cursor.fetchone()

        if not target:
            return jsonify({"status": "complete", "message": "All stamps audited."})

        # 2. Duplicate Search Logic - CHANGED TO LIMIT 5
        search_sql = """
            SELECT id, fileName, drive_url,
                   BIT_COUNT(UNHEX(fingerprint_phash) ^ UNHEX(%s)) AS dist_p,
                   BIT_COUNT(UNHEX(fingerprint_dhash) ^ UNHEX(%s)) AS dist_d,
                   BIT_COUNT(UNHEX(fingerprint_whash) ^ UNHEX(%s)) AS dist_w
            FROM stamps
            WHERE id != %s
              AND (fingerprint_phash IS NOT NULL AND fingerprint_phash != '')
            HAVING dist_p <= 25 OR dist_d <= 25 OR dist_w <= 25
            ORDER BY dist_p ASC LIMIT 10
        """
        cursor.execute(search_sql, (target['fingerprint_phash'], target['fingerprint_dhash'],
                                   target['fingerprint_whash'], target['id']))

        # CHANGED: Get all matches up to 5
        matches = cursor.fetchall()

        # 3. Apply Logic to each match
        if matches:
            found_valid_match = False

            for match in matches:
                votes = 0
                if match['dist_p'] <= 12: votes += 1
                if match['dist_d'] <= 12: votes += 1
                if match['dist_w'] <= 12: votes += 1

                confidence = 0
                if target['fileName'] == match['fileName']: confidence = 99
                elif votes == 3: confidence = 99
                elif votes == 2: confidence = 66
                elif votes == 1: confidence = 33

                if confidence > 33:
                    found_valid_match = True
                    status = "DUPLICATE" if confidence >= 66 else "UNCERTAIN"

                    # ADDED IGNORE: Prevents Primary Key crash if same target is matched multiple times
                    insert_sql = """
                        INSERT IGNORE INTO duplicate_audit (
                            stamp_id, original_id, status, dist_p, dist_d, dist_w,
                            target_fileName, target_drive_url,
                            original_fileName, original_drive_url, user_resolution
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'UNRESOLVED')
                    """
                    cursor.execute(insert_sql, (
                        target['id'], match['id'], status,
                        match['dist_p'], match['dist_d'], match['dist_w'],
                        target['fileName'], target['drive_url'],
                        match['fileName'], match['drive_url']
                    ))

            # If none of the 5 matches passed the confidence check, mark target as CLEAN
            if not found_valid_match:
                # ADDED IGNORE here too
                cursor.execute("""
                    INSERT IGNORE INTO duplicate_audit (stamp_id, status, user_resolution)
                    VALUES (%s, 'CLEAN', 'RESOLVED')
                """, (target['id'],))
        else:
            # No fuzzy matches found at all - ADDED IGNORE
            cursor.execute("""
                INSERT IGNORE INTO duplicate_audit (stamp_id, status, user_resolution)
                VALUES (%s, 'CLEAN', 'RESOLVED')
            """, (target['id'],))

        conn.commit()
        return jsonify({
            "status": "success",
            "audited_id": target['id'],
            "matches_found": len(matches) if matches else 0
        })

    except Exception as e:
        conn.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/check-cloud-duplicate', methods=['POST'])
def check_cloud_duplicate():
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    sql = ""
    params = []
    executable_sql = "" # String for console debugging

    try:
        data = request.json
        p_in = data.get('phash')
        d_in = data.get('dhash')
        w_in = data.get('whash')
        incoming_filename = data.get('fileName')

        if not p_in or not d_in or not w_in:
            return jsonify({"success": False, "error": "Missing hashes"}), 400

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # ---------------------------------------------------------
        # 1. EXACT FILENAME CHECK (Priority 1)
        # ---------------------------------------------------------
        if incoming_filename:
            sql_filename = "SELECT * FROM stamps WHERE fileName = %s"
            params_filename = (incoming_filename,)
            cursor.execute(sql_filename, params_filename)
            match_filename = cursor.fetchone()

            if match_filename:
                # Sanitize data (Date/Decimals) before returning
                for key, value in match_filename.items():
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        match_filename[key] = value.isoformat()
                    elif isinstance(value, decimal.Decimal):
                        match_filename[key] = float(value)

                return jsonify({
                    "success": True,
                    "candidates": [], # Empty as requested
                    "match_id": match_filename['fileName'],
                    "score": 100,
                    "stamp_info": match_filename, # Return full info
                    "details": {
                        "p_dist": 0,
                        "d_dist": 0,
                        "w_dist": 0,
                        "votes": 3,
                        "method": "Exact Filename Match"
                    }
                })

        # ---------------------------------------------------------
        # 2. STANDARD HASH SEARCH (Priority 2)
        # ---------------------------------------------------------
        sql = """
            SELECT *,
            BIT_COUNT(UNHEX(LPAD(fingerprint_phash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) AS dist_p,
            IFNULL(BIT_COUNT(UNHEX(LPAD(fingerprint_dhash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))), 99) AS dist_d,
            IFNULL(BIT_COUNT(UNHEX(LPAD(fingerprint_whash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))), 99) AS dist_w
            FROM stamps
            WHERE
                (fingerprint_phash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_phash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
                OR
                (fingerprint_dhash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_dhash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
                OR
                (fingerprint_whash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_whash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
            ORDER BY (dist_p + dist_d + dist_w) ASC
            LIMIT 10
        """

        params = (p_in, d_in, w_in, p_in, d_in, w_in)
        executable_sql = sql.replace('%s', "'{}'").format(*params) # Debug string

        cursor.execute(sql, params)
        matches = cursor.fetchall()

        candidate_results = []
        current_score = 0
        votes = 0

        if matches:
            for row in matches:
                current_score = 0
                votes = 0
                # --- FIX: SANITIZE ROW DATA ---
                # Convert Date/Decimal objects to strings so jsonify doesn't crash
                for key, value in row.items():
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        row[key] = value.isoformat()
                    elif isinstance(value, decimal.Decimal):
                        row[key] = float(value) # or str(value)

                if row['dist_d'] <= 20:
                    current_score += 33
                    votes += 1

                if row['dist_p'] <= 20:
                    current_score += 33
                    votes += 1

                if row['dist_w'] <= 20:
                    current_score += 33
                    votes += 1

                # --- NEW CONDITION: Only add if score is significantly high ---
                if current_score >= 33:
                    candidate_results.append({
                        "fileName": row['fileName'],
                        "image_vector": row['image_vector'],
                        "score": current_score,
                        "votes": votes,
                        "stamp_info": row,
                        "is_duplicate": current_score >= 33,
                        "debug": {
                            "dist_p": row['dist_p'],
                            "dist_d": row['dist_d'],
                            "dist_w": row['dist_w']
                        }
                    })

            # Sort Highest -> Lowest
            candidate_results.sort(key=lambda x: x['score'], reverse=True)

            return jsonify({
                "success": True,
                "candidates": candidate_results,
                "best_match": candidate_results[0] if candidate_results else None,
                "esql": executable_sql
            })

        return jsonify({"success": False, "candidates": [], "message": "No match found", "esql": executable_sql})

    except Exception as e:
        # Return the error AND the query you can run in your console
        return jsonify({
            "success": False,
            "error_msg": str(e),
            "paste_into_console": executable_sql,
            "debug_info": {
                "raw_sql": sql,
                "applied_params": params,
                "received_lengths": {
                    "p_len": len(str(p_in)) if p_in else 0,
                    "d_len": len(str(d_in)) if d_in else 0,
                    "w_len": len(str(w_in)) if w_in else 0
                }
            }
        }), 200
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()



import decimal
import datetime

@app.route('/check-cloud-duplicate-app', methods=['POST'])
def check_cloud_duplicate_app():
    # 1. Auth
    api_key = request.headers.get('X-API-KEY')
    if api_key != 'Your_Very_Secret_Stamp_Code_123':
        return jsonify({"error": "Unauthorized"}), 401

    conn = None
    cursor = None
    executable_sql = ""

    try:
        data = request.json
        p_in = data.get('phash', '')
        d_in = data.get('dhash', '')
        w_in = data.get('whash', '')

        # Validation
        if not p_in:
             return jsonify({"is_duplicate": False, "score": 0, "message": "Invalid input"})

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # 2. OPTIMIZED SQL (Single Query)
        sql = """
            SELECT *,
            BIT_COUNT(UNHEX(LPAD(fingerprint_phash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) AS dist_p,
            IFNULL(BIT_COUNT(UNHEX(LPAD(fingerprint_dhash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))), 99) AS dist_d,
            IFNULL(BIT_COUNT(UNHEX(LPAD(fingerprint_whash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))), 99) AS dist_w
            FROM stamps
            WHERE
                (fingerprint_phash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_phash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
                OR
                (fingerprint_dhash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_dhash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
                OR
                (fingerprint_whash IS NOT NULL AND BIT_COUNT(UNHEX(LPAD(fingerprint_whash, 16, '0')) ^ UNHEX(LPAD(%s, 16, '0'))) <= 25)
            ORDER BY (dist_p + dist_d + dist_w) ASC
            LIMIT 10
        """

        params = (p_in, d_in, w_in, p_in, d_in, w_in)
        executable_sql = sql.replace('%s', "'{}'").format(*params) # Debug string

        cursor.execute(sql, params)
        matches = cursor.fetchall()

        candidate_results = []
        current_score = 0
        votes = 0

        if matches:
            for row in matches:
                # --- FIX: SANITIZE ROW DATA ---
                # Convert Date/Decimal objects to strings so jsonify doesn't crash
                for key, value in row.items():
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        row[key] = value.isoformat()
                    elif isinstance(value, decimal.Decimal):
                        row[key] = float(value) # or str(value)

                # --- SCORING LOGIC ---
                current_score = 0
                votes = 0  # <--- Initialize votes

                if row['dist_d'] <= 20:
                    current_score += 33
                    votes += 1

                if row['dist_p'] <= 20:
                    current_score += 33
                    votes += 1

                if row['dist_w'] <= 20:
                    current_score += 33
                    votes += 1

                # Clean up redundancy:
                # 'row' has everything. We don't need to send image_vector twice.
                # We can construct the clean object:
                 # --- NEW CONDITION: Only add if score is significantly high ---
                if current_score >= 33:
                    candidate_results.append({
                        "fileName": row['fileName'],
                        "image_vector": row['image_vector'],
                        "score": current_score,
                        "votes": votes,
                        "stamp_info": row,
                        "is_duplicate": current_score >= 33,
                        "debug": {
                            "dist_p": row['dist_p'],
                            "dist_d": row['dist_d'],
                            "dist_w": row['dist_w']
                        }
                    })


            # Sort Highest -> Lowest
            candidate_results.sort(key=lambda x: x['score'], reverse=True)

            return jsonify({
                "success": True,
                "candidates": candidate_results,
                "best_match": candidate_results[0] if candidate_results else None,
                "esql": executable_sql
            })

        return jsonify({"success": False, "candidates": [], "message": "No match found", "esql": executable_sql})

    except Exception as e:
        print(f"Cloud Check Error: {e}")
        return jsonify({"success": False, "error": str(e), "esql": executable_sql}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()