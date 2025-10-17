# Flask app placeholder. Students extend with filters + aggregates.
# app/main.py
from flask import Flask

def create_app():
    app = Flask(__name__)

    @app.get("/")
    def index():
        return "Flask is running ✅"

    return app

# Expose a module-level `app` object so `FLASK_APP=app.main:app` works
app = create_app()
# ===== Search & Summary Routes =====
import sqlalchemy as sa
from flask import request, render_template

ENGINE = sa.create_engine(
    "mysql+pymysql://root:rootpass@db:3306/nyc311",
    pool_recycle=3600
)

def query(sql, params=None):
    with ENGINE.connect() as conn:
        return conn.execute(sa.text(sql), params or {}).mappings().all()

@app.route("/search")
def search():
    borough = (request.args.get("borough") or "").strip()
    complaint_type = (request.args.get("complaint_type") or "").strip()
    start = (request.args.get("start") or "").strip()
    end = (request.args.get("end") or "").strip()
    page = max(int(request.args.get("page", 1) or 1), 1)
    page_size = 20
    offset = (page - 1) * page_size

    where, params = ["1=1"], {}
    if borough:
        where.append("borough = :borough"); params["borough"] = borough
    if complaint_type:
        where.append("complaint_type = :ctype"); params["ctype"] = complaint_type
    if start:
        where.append("created_date >= :start"); params["start"] = start
    if end:
        where.append("created_date < :end"); params["end"] = end

    where_sql = " AND ".join(where)

    rows = query(f"""
        SELECT unique_key, created_date, closed_date, agency, complaint_type, descriptor, borough
        FROM service_requests
        WHERE {where_sql}
        ORDER BY created_date DESC
        LIMIT :limit OFFSET :offset
    """, {**params, "limit": page_size, "offset": offset})

    total = query(f"SELECT COUNT(*) AS c FROM service_requests WHERE {where_sql}", params)[0]["c"]
    total_pages = max((total + page_size - 1) // page_size, 1)

    return render_template("search.html",
        rows=rows, page=page, total=total, total_pages=total_pages,
        borough=borough, complaint_type=complaint_type, start=start, end=end
    )

@app.route("/summary")
def summary():
    rows = query("""
        SELECT borough, COUNT(*) AS n
        FROM service_requests
        GROUP BY borough
        ORDER BY n DESC
    """)
    total = sum(r["n"] for r in rows)
    return render_template("summary.html", rows=rows, total=total)
