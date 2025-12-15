# -*- coding: utf-8 -*-
"""
Excel → Odoo Product Import (Odoo 19) — compacte, robuuste versie

Nieuw in deze versie (SSE/stream fixes):
- logs_stream(): géén kunstmatige 10s-break; keepalive comment-pings; done exact één keer;
  geen writes in finally → voorkomt "RuntimeError: generator ignored GeneratorExit".
- JobState.mark_done(): pusht eerst een 'done' event en daarna '__END__' sentinel.
- Server-side geen “Verbonden …” log meer uit de generator (client toont het bij 'open').

Overige highlights blijven:
- Lookup volgorde: barcode → naam → anders create met ALLE velden (incl. supplierinfo).
- RunCache: tag_by_name toevoeging (bugfix).
- Stock updates via inventory flow: inventory_quantity + action_apply_inventory (batch).
- Veilig images (HTTP pool_block=True, stream=True, futures flush).
- Taxes: percent vs fixed + default VAT 21%.
- Put-away: autodetect velden + path helpers.
- UoM & routes resolvers + aliasing.
- Translations: base/lang ontkoppeld; duplicates tegengehouden.
"""

import os, uuid, time, json, logging, xmlrpc.client, requests, re, base64, threading, random
from io import BytesIO
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from collections import defaultdict

import pandas as pd
from PIL import Image

from flask import Flask, request, render_template, redirect, url_for, session, jsonify, Response
from flask_session import Session
from werkzeug.utils import secure_filename
from werkzeug.exceptions import ClientDisconnected

# -----------------------------------------------------------------------------
# Flask & logging
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.environ.get("APP_SECRET", "supersecretkey")
app.config["SESSION_TYPE"] = "filesystem"
app.config["JSON_AS_ASCII"] = False
Session(app)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Globals / tuning
# -----------------------------------------------------------------------------
UPLOADS = "uploads"
os.makedirs(UPLOADS, exist_ok=True)

def env_flag(key: str, default=False):
    v = os.environ.get(key)
    if v is None:
        return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

GLOBAL_FAST_MODE = env_flag("FAST_MODE", False)

# HTTP/Image tuning
IMAGE_POOL_MAX = int(os.environ.get("IMAGE_POOL_MAX", "64"))
SESSION_HTTP = requests.Session()
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=IMAGE_POOL_MAX,
    pool_maxsize=IMAGE_POOL_MAX,
    max_retries=0,
    pool_block=True,  # NIET laten vallen → blocken
)
SESSION_HTTP.mount("http://", _adapter)
SESSION_HTTP.mount("https://", _adapter)
SESSION_HTTP.headers.update({
    "User-Agent": "Excel->Odoo Importer/3.3",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
})

MAX_IMAGE_WORKERS = int(os.environ.get("IMAGE_WORKERS", "8"))  # begrensd op IMAGE_POOL_MAX
MAX_IMG_PX = int(os.environ.get("MAX_IMG_PX", "1024"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))
MAX_IMG_BYTES = int(os.environ.get("MAX_IMG_BYTES", str(6 * 1024 * 1024)))  # 6MB

# Chunking
LOOKUP_CHUNK = int(os.environ.get("LOOKUP_CHUNK", "300"))
CREATE_CHUNK = int(os.environ.get("CREATE_CHUNK", "100"))

# Progress
PROGRESS_MIN_INTERVAL = float(os.environ.get("PROGRESS_MIN_INTERVAL", "0.25"))  # s
PROGRESS_EVERY_ROWS = int(os.environ.get("PROGRESS_EVERY_ROWS", "100"))

# URL normalization
URL_NEEDS_SCHEME = re.compile(r"^(?:www\.)[^\s]+", re.I)

def _normalize_url(u: str) -> str:
    if not u: 
        return u
    s = str(u).strip()
    if URL_NEEDS_SCHEME.match(s):
        return "https://" + s
    if s.startswith("//"):
        return "https:" + s
    return s

# -----------------------------------------------------------------------------
# XML-RPC transport (keep-alive + pooling + gzip + 429 jitter)
# -----------------------------------------------------------------------------
class RequestsTransport(xmlrpc.client.Transport):
    user_agent = "Excel->Odoo Importer XMLRPC/3.3"
    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=max(32, IMAGE_POOL_MAX),
            pool_maxsize=max(32, IMAGE_POOL_MAX),
            max_retries=0,
            pool_block=True,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate, br",
        })

    def request(self, host, handler, request_body, verbose=False):
        host = host.rstrip("/")
        handler = handler.lstrip("/")
        url = f"{'https://' if not host.startswith('http') else ''}{host}/{handler}"
        headers = {"Content-Type": "text/xml"}
        backoff = 1.6
        for attempt in range(6):
            try:
                with self.session.post(
                    url, data=request_body, headers=headers,
                    timeout=90, allow_redirects=False
                ) as resp:
                    if resp.is_redirect or resp.status_code in (301,302,303,307,308):
                        raise Exception(f"Unexpected redirect to {resp.headers.get('Location')}")
                    if resp.status_code == 429:
                        ra = resp.headers.get("Retry-After")
                        if ra and re.fullmatch(r"\d+(\.\d+)?", ra or ""):
                            time.sleep(float(ra))
                        else:
                            time.sleep((backoff ** attempt) * (0.75 + 0.5*random.random()))
                        continue
                    resp.raise_for_status()
                    if "text/xml" not in (resp.headers.get("Content-Type") or ""):
                        raise Exception(f"Unexpected content type: {resp.headers.get('Content-Type')}\n{resp.text[:400]}")
                    p, u = self.getparser()
                    p.feed(resp.content)
                    return u.close()
            except requests.exceptions.RequestException as e:
                if getattr(e.response, "status_code", None) == 429:
                    time.sleep((backoff ** attempt) * (0.75 + 0.5*random.random()))
                    continue
                if attempt >= 5:
                    raise
                time.sleep(0.3 * (attempt + 1))

# -----------------------------------------------------------------------------
# SSE jobstate
# -----------------------------------------------------------------------------
class JobState:
    def __init__(self, job_id=None):
        self.job_id = job_id
        self.queue = Queue()
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.processed = 0
        self.total = 0
        self.done = False
        self.result_messages = []
        self.error = None
        self._last_push = 0.0
        # fase
        self.phase = None
        self.phase_processed = 0
        self.phase_total = 0
        # cancel
        self._cancelled = False
        self._last_save = 0.0

    def push(self, text):
        try:
            self.queue.put_nowait(text)
        except Exception:
            pass
        now = time.time()
        if now - self._last_save > 2.0:
            self.save(); self._last_save = now

    def set_progress(self, processed=None, total=None):
        with self.lock:
            if processed is not None: self.processed = int(processed)
            if total is not None: self.total = int(total)
            if time.time() - self._last_save > 2.0:
                self.save(); self._last_save = time.time()

    def set_phase(self, name=None, processed=None, total=None):
        with self.lock:
            if name is not None: self.phase = name
            if processed is not None: self.phase_processed = int(processed)
            if total is not None: self.phase_total = int(total)
            self.save(); self._last_save = time.time()
        payload = {"phase": self.phase, "processed": self.phase_processed, "total": self.phase_total}
        self.push(sse_format("phase", payload))

    def maybe_progress(self, force=False):
        now = time.time()
        if force or (now - self._last_push) >= PROGRESS_MIN_INTERVAL:
            self._last_push = now
            self.push(sse_format("progress", {
                "processed": self.processed,
                "total": self.total,
                "phase": self.phase,
                "phase_processed": self.phase_processed,
                "phase_total": self.phase_total,
            }))

    def mark_done(self):
        with self.lock:
            self.done = True
            self.save()
        # done eerst uitsturen, dan sentinel
        try:
            self.queue.put_nowait(sse_format("done", {"ok": self.error is None, "error": self.error}))
        except Exception:
            pass
        try:
            self.queue.put_nowait("__END__")
        except Exception:
            pass

    def cancel(self):
        with self.lock:
            self._cancelled = True
        self.save()

    def is_cancelled(self):
        with self.lock:
            return self._cancelled

    def to_dict(self):
        return {
            "job_id": self.job_id, "start_time": self.start_time,
            "processed": self.processed, "total": self.total,
            "cancel_requested": self._cancelled,
            "phase": self.phase, "phase_processed": self.phase_processed, "phase_total": self.phase_total,
            "done": self.done, "result_messages": self.result_messages, "error": self.error,
        }

    @classmethod
    def from_dict(cls, data):
        job = cls(job_id=data.get("job_id"))
        job.start_time = data.get("start_time", time.time())
        job.processed = data.get("processed", 0)
        job.total = data.get("total", 0)
        job._cancelled = data.get("cancel_requested", False)
        job.phase = data.get("phase")
        job.phase_processed = data.get("phase_processed", 0)
        job.phase_total = data.get("phase_total", 0)
        job.done = data.get("done", False)
        job.result_messages = data.get("result_messages", [])
        job.error = data.get("error")
        return job

    def save(self):
        if not self.job_id: return
        try:
            with open(os.path.join(JOBS_DIR, f"{self.job_id}.json"), "w", encoding="utf-8") as f:
                json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save job {self.job_id}: {e}")

JOBS_DIR = "jobs"
os.makedirs(JOBS_DIR, exist_ok=True)
JOBS = {}

def get_job(job_id) -> JobState:
    if not job_id: return None
    if job_id in JOBS: return JOBS[job_id]
    path = os.path.join(JOBS_DIR, f"{job_id}.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            job = JobState.from_dict(data)
            job_age = time.time() - job.start_time
            if not job.done and job_age > 60:
                job.error = "Job onderbroken door server herstart (mogelijk geheugenlimiet)."
                job.done = True
                job.result_messages.append("Job proces is gestopt.")
                job.save()
            JOBS[job_id] = job
            return job
        except Exception as e:
            logger.error(f"Error loading job {job_id}: {e}")
            return None
    return None

def sse_format(name, payload):
    if not isinstance(payload, str):
        payload = json.dumps(payload, ensure_ascii=False)
    return f"event: {name}\ndata: {payload}\n\n"

# -----------------------------------------------------------------------------
# Helpers & constants
# -----------------------------------------------------------------------------
MAX_XMLRPC_INT = 2**31 - 1
TRANSLATABLE_FIELDS = {"name", "description_sale", "website_description"}

TRANSLATION_COL_REGEXES = [
    re.compile(r'^\s*(name|description_sale|website_description)\s*\(([a-zA-Z]{2}[_-][a-zA-Z]{2})\)\s*$'),
    re.compile(r'^\s*(name|description_sale|website_description)\s*\[([a-zA-Z]{2}[_-][a-zA-Z]{2})\]\s*$'),
]

CATEGORY_SENTINELS = {"", "-", "van categorie", "from category", "inherit", "category",
                      "use category", "categorie", "categorie-instelling", "default", "none"}

DETAILED_TYPE_ALIASES = {
    "product": {"product", "goods", "goederen", "storable", "stockable"},
    "consu": {"consu", "consumable", "consumables", "verbruiksartikel", "verbruiksartikelen"},
    "service": {"service", "diensten", "dienst"},
}

ROUTE_ALIASES = {
    "kopen": ["Kopen", "Buy"],
    "buy": ["Buy", "Kopen"],
    "mto": ["MTO", "Make To Order", "Replenish on Order (MTO)", "Aanvullen op bestelling (MTO)", "Aanvullen per order (MTO)"],
    "aanvullen op bestelling": ["Aanvullen op bestelling (MTO)", "Replenish on Order (MTO)", "Make To Order", "MTO"],
    "aanvullen per order": ["Aanvullen per order (MTO)", "Aanvullen op bestelling (MTO)", "Replenish on Order (MTO)", "Make To Order", "MTO"],
}

UOM_ALIASES = {
    "pcs": ["pc", "piece", "pieces", "st", "st.", "stuk", "stuks", "unit", "units"],
    "kg": ["kilogram", "kilograms", "kilogramme", "kg.", "kgs"],
    "g": ["gram", "grams", "gr", "g."],
    "mg": ["milligram", "milligrams"],
    "lb": ["pound", "pounds", "lbs", "lb."],
    "oz": ["ounce", "ounces", "oz."],
    "l": ["liter", "liters", "litre", "litres", "l."],
    "ml": ["milliliter", "milliliters", "millilitre", "millilitres", "ml."],
    "m": ["meter", "meters", "metre", "metres", "m."],
    "cm": ["centimeter", "centimeters", "centimetre", "centimetres", "cm."],
    "mm": ["millimeter", "millimeters", "millimetre", "millimetres", "mm."],
    "m²": ["m2", "sqm", "square meter", "square meters"],
    "m³": ["m3", "cubic meter", "cubic meters"],
    "h": ["hour", "hours", "uur", "u"],
    "day": ["days", "dag", "dagen"],
    "month": ["months", "maand", "maanden"],
}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def parse_decimal(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    if "," in s and "." not in s:
        s = s.replace(" ", "").replace(".", "").replace(",", ".")
    else:
        s = s.replace(" ", "")
    try:
        d = Decimal(s)
        return d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP).normalize()
    except InvalidOperation:
        try:
            d = Decimal(str(float(s)))
            return d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP).normalize()
        except Exception:
            return None

def format_decimal_for_name(d: Decimal) -> str:
    if d is None: return "0"
    s = format(d.normalize(), "f")
    if "." in s: s = s.rstrip("0").rstrip(".")
    return s if s else "0"

def preserve_leading_zeros_str(raw):
    if raw is None:
        return ""
    s = str(raw).strip()
    if s.endswith(".0") and re.fullmatch(r"^\d+(\.0+)?$", s):
        s = s.split(".")[0]
    return s

# -----------------------------------------------------------------------------
# Cache structs
# -----------------------------------------------------------------------------
class RunCache:
    def __init__(self):
        self.fields_meta = {}
        self.m2o = {}
        self.m2m_split = {}
        self.categories = {}
        self.uom_by_norm = {}
        self.uom_alias_by_norm = {}
        self.tax_by_name = {}
        self.partner_by_name = {}
        self.account_cache = {}
        self.routes = {}
        self.routes_loaded = False
        self.wh_code_id = {}
        self.wh_roots = {}
        self.loc_path = {}
        self.putaway_seen = {}
        self.tax_percent_by_amount = {}
        self.image_by_url = {}
        self.barcode_to_id = {}
        self.name_to_id = {}
        self.tag_by_name = {}   # <-- FIX (bestond niet, gebruikt bij product_tag_ids)

CACHE = None

# -----------------------------------------------------------------------------
# Odoo helpers
# -----------------------------------------------------------------------------
def retry(func, *args, retries=6, backoff=1.7, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            if getattr(e.response, "status_code", None) == 429:
                time.sleep((backoff ** i) * (0.75 + 0.5*random.random()))
                continue
            if i >= retries - 1:
                raise
            time.sleep(0.2 * (i + 1))
        except xmlrpc.client.Fault:
            raise

def model_has_field(models, db, uid, key, model, field_name):
    try:
        fget = retry(models.execute_kw, db, uid, key, model, "fields_get", [],
                     {"attributes": ["type","relation","selection"]})
        return field_name in fget
    except Exception:
        return False

def _fields_get_cached(models, db, uid, key, model):
    global CACHE
    if model in CACHE.fields_meta:
        return CACHE.fields_meta[model]
    meta = retry(models.execute_kw, db, uid, key, model, "fields_get", [],
                 {"attributes": ["type", "relation", "string", "selection"]})
    CACHE.fields_meta[model] = meta
    return meta

def normalize_lang_code(code: str) -> str:
    code = (code or "").replace("-", "_")
    parts = code.split("_")
    return f"{parts[0].lower()}_{parts[1].upper()}" if len(parts) == 2 else code

def company_ctx(company_id, lang=None):
    ctx = {"tracking_disable": True, "mail_notrack": True, "prefetch_fields": False}
    if lang: ctx["lang"] = normalize_lang_code(lang)
    if company_id:
        ctx["force_company"] = int(company_id)
        ctx["allowed_company_ids"] = [int(company_id)]
    return ctx

def get_companies(models, db, uid, key):
    try:
        recs = retry(models.execute_kw, db, uid, key, "res.company", "search_read",
                     [[]], {"fields": ["id", "name"], "limit": 200})
        recs.sort(key=lambda r: (r.get("name") or "").lower())
        return recs
    except Exception:
        return []

def _coerce_id(x):
    if isinstance(x, int):
        return x
    if isinstance(x, (list, tuple)):
        if not x: raise ValueError("Empty sequence cannot be an id")
        return _coerce_id(x[0])
    try:
        return int(Decimal(str(x)))
    except Exception:
        raise ValueError(f"Invalid id: {x!r}")

def ensure_ids_list(ids):
    if ids is None: return []
    if isinstance(ids, (list, tuple)): return [_coerce_id(i) for i in ids]
    return [_coerce_id(ids)]

def is_category_inherit(val) -> bool:
    if val is None: return False
    if isinstance(val, (int, float)): return False
    return str(val).strip().lower() in CATEGORY_SENTINELS

# -----------------------------------------------------------------------------
# UoM resolver
# -----------------------------------------------------------------------------
class UoMResolver:
    def load(self, models, db, uid, key):
        global CACHE
        CACHE.uom_alias_by_norm.clear()
        for k, arr in UOM_ALIASES.items():
            CACHE.uom_alias_by_norm[_norm(k)] = _norm(k)
            for a in arr:
                CACHE.uom_alias_by_norm[_norm(a)] = _norm(k)
        try:
            uoms = retry(models.execute_kw, db, uid, key, "uom.uom", "search_read",
                         [[]], {"fields": ["id", "name", "display_name"], "limit": 10000})
        except Exception as e:
            logging.warning(f"UoM load failed: {e}")
            uoms = []
        for r in uoms:
            for candidate in (r.get("name"), r.get("display_name")):
                if candidate:
                    CACHE.uom_by_norm.setdefault(_norm(candidate), r["id"])

    def get(self, models, db, uid, key, raw, ctx=None):
        global CACHE
        if not raw: return None
        raw_s = str(raw).strip()
        n = _norm(raw_s)
        if n in CACHE.uom_by_norm:
            return CACHE.uom_by_norm[n]
        if n in CACHE.uom_alias_by_norm:
            alias_key = CACHE.uom_alias_by_norm[n]
            if alias_key in CACHE.uom_by_norm:
                return CACHE.uom_by_norm[alias_key]
        stripped = re.sub(r"\(.*?\)", "", raw_s).strip()
        if stripped and _norm(stripped) in CACHE.uom_by_norm:
            return CACHE.uom_by_norm[_norm(stripped)]
        try:
            res = retry(models.execute_kw, db, uid, key, "uom.uom", "name_search",
                        [raw_s], {"operator":"ilike","limit":1,"context":(ctx or {})})
            if res:
                hit = int(_coerce_id(res[0][0]))
                CACHE.uom_by_norm[_norm(raw_s)] = hit
                return hit
        except Exception:
            pass
        try:
            ids = retry(models.execute_kw, db, uid, key, "uom.uom", "search",
                        [[("name","ilike",raw_s)]], {"limit":1,"context":(ctx or {})})
            if ids:
                hit = int(_coerce_id(ids[0]))
                CACHE.uom_by_norm[_norm(raw_s)] = hit
                return hit
        except Exception:
            pass
        return None

UOM = UoMResolver()

# -----------------------------------------------------------------------------
# Relation resolvers & m2m
# -----------------------------------------------------------------------------
def _coerce_bool(v):
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return s in ("1","true","yes","ja","y","on")

def _coerce_float(v):
    d = parse_decimal(v)
    return float(d) if d is not None else None

def _resolve_selection(value, selection_list):
    s = str(value).strip()
    low = s.lower()
    for k, _ in (selection_list or []):
        if str(k).lower() == low:
            return k
    for k, lbl in (selection_list or []):
        if str(lbl or "").strip().lower() == low:
            return k
    for k, lbl in (selection_list or []):
        if str(lbl or "").strip().lower().startswith(low):
            return k
    return None

def _relation_char_buckets(meta_fields):
    codeish, nameish, others = [], [], []
    for fname, meta in (meta_fields or {}).items():
        if (meta or {}).get("type") != "char": continue
        low = fname.lower()
        if any(tok in low for tok in ("code","ref","sku","nummer","nr","cod","key","group","groep")):
            codeish.append(fname)
        elif low == "name" or "naam" in low or low == "display_name":
            nameish.append(fname)
        else:
            others.append(fname)
    if "code" in codeish:
        codeish.insert(0, codeish.pop(codeish.index("code")))
    return codeish, nameish, others

def _name_search(models, db, uid, key, relation, value, company_id=None, limit=1):
    try:
        res = retry(models.execute_kw, db, uid, key, relation, "name_search",
            [str(value)], {"operator":"ilike","limit":int(limit),"context":company_ctx(company_id)})
        if res:
            return [int(_coerce_id(r[0])) for r in res]
    except Exception:
        pass
    return []

def _search_on_fields(models, db, uid, key, relation, field_name, value, company_id=None, limit=1, exact=True):
    op = "=" if exact else "ilike"
    dom = [(field_name, op, value if exact else str(value))]
    try:
        ids = retry(models.execute_kw, db, uid, key, relation, "search",
            [dom], {"limit":int(limit),"context":company_ctx(company_id)})
        return [int(_coerce_id(i)) for i in ids] if ids else []
    except Exception:
        return []

def _resolve_many2one(models, db, uid, key, relation, raw, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(float(str(raw)))
    except Exception:
        pass
    value = str(raw).strip()
    kn = (relation, _norm(value), int(company_id or 0))
    if kn in CACHE.m2o:
        return CACHE.m2o[kn]
    hit = _name_search(models, db, uid, key, relation, value, company_id=company_id, limit=1)
    if hit:
        CACHE.m2o[kn] = hit[0]
        return hit[0]
    try:
        rel_fields = _fields_get_cached(models, db, uid, key, relation)
    except Exception:
        rel_fields = {}
    codeish, nameish, others = _relation_char_buckets(rel_fields)
    for bucket in (codeish, nameish, others):
        for f in bucket:
            ids = _search_on_fields(models, db, uid, key, relation, f, value, company_id, limit=1, exact=True)
            if ids:
                CACHE.m2o[kn] = ids[0]
                return ids[0]
    for bucket in (codeish, nameish, others):
        for f in bucket:
            ids = _search_on_fields(models, db, uid, key, relation, f, value, company_id, limit=1, exact=False)
            if ids:
                CACHE.m2o[kn] = ids[0]
                return ids[0]
    if "name" in (rel_fields or {}):
        ids = _search_on_fields(models, db, uid, key, relation, "name", value, company_id, limit=1, exact=False)
        if ids:
            CACHE.m2o[kn] = ids[0]
            return ids[0]
    return None

def _prefetch_routes(models, db, uid, key, company_id=None):
    global CACHE
    if CACHE.routes_loaded: return
    try:
        recs = retry(models.execute_kw, db, uid, key, "stock.route", "search_read",
            [[]], {"fields":["id","name"], "limit": 10000, "context": company_ctx(company_id)})
        for r in recs or []:
            nm = r.get("name") or ""
            if nm: CACHE.routes[_norm(nm)] = int(_coerce_id(r["id"]))
    except Exception:
        pass
    CACHE.routes_loaded = True

def _stock_route_candidates(label: str):
    s = str(label or "").strip(); low = s.lower()
    cands = [s]
    for key, arr in ROUTE_ALIASES.items():
        if key in low or s in arr:
            cands.extend(arr)
    if "mto" in low: cands.extend(ROUTE_ALIASES["mto"])
    if "kopen" in low or "buy" in low: cands.extend(ROUTE_ALIASES["kopen"])
    return list(dict.fromkeys([c for c in cands if c]))

def _resolve_stock_route_ids(models, db, uid, key, raw, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "": return []
    _prefetch_routes(models, db, uid, key, company_id=company_id)
    items = re.split(r"[,\n;\|]", str(raw))
    out = []
    for p in (t.strip() for t in items):
        if not p: continue
        n = _norm(p)
        rid = CACHE.routes.get(n)
        if rid:
            if rid not in out: out.append(rid); continue
        found = None
        for cand in _stock_route_candidates(p):
            rid = CACHE.routes.get(_norm(cand))
            if rid: found = rid; break
        if found and found not in out: out.append(found)
    return out

def _resolve_many2many(models, db, uid, key, relation, raw, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "": return []
    if relation == "stock.route":
        return _resolve_stock_route_ids(models, db, uid, key, raw, company_id=company_id)
    items = re.split(r"[,\n;\|]", str(raw))
    ids = []
    for p in (t.strip() for t in items):
        if not p: continue
        kn = (relation, _norm(p), int(company_id or 0))
        if kn in CACHE.m2m_split:
            mid = CACHE.m2m_split[kn]
        else:
            mid = _resolve_many2one(models, db, uid, key, relation, p, company_id)
            if mid: CACHE.m2m_split[kn] = int(_coerce_id(mid))
        if mid:
            mid = int(_coerce_id(mid))
            if mid not in ids: ids.append(mid)
    return ids

# --- Resolver-suffix parsing (voor m2o/m2m: ":name" / ":id" / ":path") ---
def _split_field_and_resolver(field_key: str):
    """
    Mapping-keys ondersteunen 'veld:suffix' (suffix in {'name','id','path'}).
    Retourneert (pure_field, resolver).
    """
    if not field_key:
        return field_key, None
    s = str(field_key).strip()
    if ":" in s:
        base, suffix = s.rsplit(":", 1)
        suf = suffix.strip().lower()
        if suf in ("name", "id", "path"):
            return base.strip(), suf
    return s, None


def _model_has_parent_hierarchy(models, db, uid, key, model_name: str) -> bool:
    """
    Checkt of het doelmodel een 'parent_id' heeft die naar zichzelf wijst.
    Nodig om padnotatie ('A/B/C') te kunnen aanmaken.
    """
    try:
        meta = _fields_get_cached(models, db, uid, key, model_name)
        f = meta.get("parent_id")
        return bool(f and f.get("type") == "many2one" and f.get("relation") == model_name)
    except Exception:
        return False


def get_or_create_path(models, db, uid, key, model_name: str, path: str, company_id=None):
    """
    Generieke variant van get_or_create_category() voor elk model met parent_id → zichzelf.
    Voorbeeld: 'product.public.category', 'pos.category', custom 'x_category_model', …
    Path met '/' maakt/zoekt hiërarchisch.
    """
    raw = (path or "").replace("\\", "/")
    parts = [p.strip() for p in raw.split("/") if p and p.strip()]
    if not parts:
        return False

    if not _model_has_parent_hierarchy(models, db, uid, key, model_name):
        # Geen hiërarchisch model; probeer op 'name' vlak
        ids = retry(models.execute_kw, db, uid, key, model_name, "search",
                    [[("name", "=", raw)]], {"limit": 1, "context": company_ctx(company_id)})
        if ids:
            return int(_coerce_id(ids[0]))
        return retry(models.execute_kw, db, uid, key, model_name, "create",
                     [[{"name": raw}]], {"context": company_ctx(company_id)})

    parent_id = False
    for seg in parts:
        dom = [("name", "=", seg)]
        if parent_id:
            dom.append(("parent_id", "=", int(_coerce_id(parent_id))))
        # Company-filter indien het model company_id heeft
        try:
            if company_id and model_has_field(models, db, uid, key, model_name, "company_id"):
                dom.append(("company_id", "in", [int(company_id), False]))
        except Exception:
            pass

        ids = retry(models.execute_kw, db, uid, key, model_name, "search", [dom],
                    {"limit": 1, "context": company_ctx(company_id)})
        if ids:
            parent_id = ids[0]
            continue

        # Create segment
        vals = {"name": seg, "parent_id": int(_coerce_id(parent_id)) if parent_id else False}
        try:
            if company_id and model_has_field(models, db, uid, key, model_name, "company_id"):
                vals["company_id"] = int(company_id)
        except Exception:
            pass

        parent_id = retry(models.execute_kw, db, uid, key, model_name, "create",
                          [[vals]], {"context": company_ctx(company_id)})

    return int(_coerce_id(parent_id))

def resolve_dynamic_field(models, db, uid, key, field_name, raw, company_id=None):
    # NEW: support 'field:suffix' in mapping keys (suffix in {'name','id','path'})
    pure_field_name, resolver = _split_field_and_resolver(field_name)

    meta = _fields_get_cached(models, db, uid, key, "product.template").get(pure_field_name) or {}
    ftype = meta.get("type")
    if not ftype:
        return (False, raw)

    # Primitives
    if ftype in ("char", "text", "html", "binary"):
        return (False, None if raw == "" else raw)
    if ftype == "boolean":
        return (False, _coerce_bool(raw))
    if ftype == "integer":
        try:
            return (False, int(float(str(raw).replace(",", "."))))
        except Exception:
            return (False, None)
    if ftype in ("float", "monetary"):
        return (False, _coerce_float(raw))
    if ftype == "selection":
        sel_key = _resolve_selection(raw, meta.get("selection"))
        return (False, sel_key)

    # Many2one
    if ftype == "many2one":
        rel = meta.get("relation")

        # UoM shortcut
        if pure_field_name in ("uom_id", "uom_po_id"):
            uom_id = UOM.get(models, db, uid, key, raw, company_ctx(company_id))
            return (False, uom_id)

        val = str(raw or "").strip()
        if not val:
            return (False, None)

        if resolver == "id":
            try:
                return (False, _coerce_id(val))
            except Exception:
                return (False, None)

        if resolver == "name":
            hits = _name_search(models, db, uid, key, rel, val, company_id=company_id, limit=1)
            return (False, (hits and hits[0]) or None)

        if resolver == "path":
            # Padnotatie "A/B/C" → generiek voor elk parent_id-model
            try:
                rid = get_or_create_path(models, db, uid, key, rel, val, company_id=company_id)
                return (False, rid)
            except Exception:
                return (False, None)

        # default (huidige slimme resolver)
        mid = _resolve_many2one(models, db, uid, key, rel, val, company_id)
        return (False, mid)

    # Many2many / One2many
    if ftype in ("many2many", "one2many"):
        rel = meta.get("relation")
        s = str(raw or "")
        if not s.strip():
            return (True, [])

        # Split per item; elk item kan een pad zijn (bij :path) of naam/ID
        items = [p.strip() for p in re.split(r"[,\n;\|]", s) if p and p.strip()]
        if not items:
            return (True, [])

        if resolver == "id":
            try:
                ids = [_coerce_id(x) for x in items]
                return (True, ids)
            except Exception:
                return (True, [])

        if resolver == "name":
            ids = []
            for it in items:
                hits = _name_search(models, db, uid, key, rel, it, company_id=company_id, limit=1)
                if hits:
                    ids.append(int(_coerce_id(hits[0])))
            return (True, ids)

        if resolver == "path":
            # Elk item is een pad "A/B/C"
            ids = []
            for it in items:
                rid = get_or_create_path(models, db, uid, key, rel, it, company_id=company_id)
                if rid:
                    ids.append(int(_coerce_id(rid)))
            return (True, ids)

        # default (jouw bestaande gedrag incl. routes)
        mids = _resolve_many2many(models, db, uid, key, rel, s, company_id)
        return (True, mids)

    # Fallback
    return (False, raw)

# -----------------------------------------------------------------------------
# Accounts / Taxes / Categories / Suppliers
# -----------------------------------------------------------------------------
def get_default_lang(models, db, uid, key) -> str:
    try:
        rec = retry(models.execute_kw, db, uid, key, "res.users", "read", [[uid], ["lang","company_id"]])
        code = (rec[0].get("lang") if rec else "") or "en_US"
        return normalize_lang_code(code)
    except Exception:
        return "en_US"

def get_user_company_id(models, db, uid, key):
    try:
        rec = retry(models.execute_kw, db, uid, key, "res.users", "read", [[uid], ["company_id"]])
        if rec and rec[0].get("company_id"):
            return _coerce_id(rec[0]["company_id"])
    except Exception:
        pass
    return None

def get_active_languages(models, db, uid, key):
    try:
        recs = retry(models.execute_kw, db, uid, key, "res.lang", "search_read",
                     [[("active", "=", True)]], {"fields": ["code", "name"], "limit": 200})
        langs = []
        for r in recs:
            code = normalize_lang_code(r.get("code") or "")
            if code: langs.append((code, r.get("name") or code))
        return langs or [("nl_BE", "Dutch (Belgium)"), ("fr_BE", "French (Belgium)"), ("en_US", "English (US)")]
    except Exception:
        return [("nl_BE", "Dutch (Belgium)"), ("fr_BE", "French (Belgium)"), ("en_US", "English (US)")]

def _account_schema(models, db, uid, key):
    has_account_type = model_has_field(models, db, uid, key, "account.account", "account_type")
    has_user_type = model_has_field(models, db, uid, key, "account.account", "user_type_id")
    return has_account_type, has_user_type

def _find_account_type_id(models, db, uid, key, kind):
    try:
        types = retry(models.execute_kw, db, uid, key, "account.account.type", "search_read",
                      [[("internal_group", "=", kind)]], {"fields": ["id"], "limit": 1})
        if types: return types[0]["id"]
    except Exception:
        pass
    name_q = "Income" if kind == "income" else "Expenses"
    try:
        types = retry(models.execute_kw, db, uid, key, "account.account.type", "search",
                      [[("name", "ilike", name_q)]], {"limit": 1})
        if types: return types[0]
    except Exception:
        pass
    return None

def _normalize_account_code(raw):
    s = str(raw).strip()
    if s.isdigit() and len(s) < 6:
        return s.zfill(6)
    return s

def find_or_create_account(models, db, uid, key, raw, kind, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "":
        return None
    k = (str(raw), kind, int(company_id or 0))
    if k in CACHE.account_cache:
        return CACHE.account_cache[k]

    s = str(raw).strip()
    dom_company = []
    try:
        if company_id and model_has_field(models, db, uid, key, "account.account", "company_id"):
            dom_company = [("company_id", "in", [int(company_id), False])]
    except Exception:
        dom_company = []

    dom = dom_company + [("code", "=", s)]
    ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1})
    if ids:
        CACHE.account_cache[k] = ids[0]; return ids[0]

    s6 = _normalize_account_code(s)
    if s6 != s:
        dom = dom_company + [("code", "=", s6)]
        ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1})
        if ids:
            CACHE.account_cache[k] = ids[0]; return ids[0]

    dom = dom_company + [("code", "ilike", f"{s}%")]
    ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1, "order": "code asc"})
    if ids:
        CACHE.account_cache[k] = ids[0]; return ids[0]

    dom = dom_company + [("name", "ilike", s)]
    ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1})
    if ids:
        CACHE.account_cache[k] = ids[0]; return ids[0]

    has_account_type, has_user_type = _account_schema(models, db, uid, key)
    vals = {"code": s6, "name": f"{'Income' if kind=='income' else 'Expense'} {s}", "reconcile": False}
    if company_id and model_has_field(models, db, uid, key, "account.account", "company_id"):
        vals["company_id"] = int(_coerce_id(company_id))

    if has_account_type:
        vals["account_type"] = "income" if kind == "income" else "expense"
    elif has_user_type:
        at_id = _find_account_type_id(models, db, uid, key, "income" if kind == "income" else "expense")
        if at_id: vals["user_type_id"] = int(at_id)

    try:
        new_id = retry(models.execute_kw, db, uid, key, "account.account", "create", [[vals]])
        CACHE.account_cache[k] = new_id
        return new_id
    except xmlrpc.client.Fault as e:
        logging.warning(f"Kon account niet creëren voor '{s}' ({kind}): {e}")
        return None

def _get_or_create_tax_group(models, db, uid, key, company_id=None):
    gid = retry(models.execute_kw, db, uid, key, "account.tax.group", "search",
                [[("name", "=", "All")]], {"limit": 1, "context": company_ctx(company_id)})
    if gid: return gid[0]
    return retry(models.execute_kw, db, uid, key, "account.tax.group", "create",
                 [[{"name": "All"}]], {"context": company_ctx(company_id)})

def get_or_create_tax(models, db, uid, key, name, company_id=None, amount=None, amount_type=None):
    global CACHE
    s = str(name or "").strip()
    k = (_norm(s), int(company_id or 0))
    if k in CACHE.tax_by_name:
        return CACHE.tax_by_name[k][0]
    ids = retry(models.execute_kw, db, uid, key, "account.tax", "search",
                [[("name", "=", s)]], {"limit": 1, "context": company_ctx(company_id)})
    if ids:
        rec = retry(models.execute_kw, db, uid, key, "account.tax", "read",
                    [ensure_ids_list(ids[0]), ["amount_type"]],
                    {"context": company_ctx(company_id)})
        amt_type = (rec and rec[0].get("amount_type")) or None
        CACHE.tax_by_name[k] = (ids[0], amt_type)
        return ids[0]
    if amount_type is None:
        return None

    grp_id = _get_or_create_tax_group(models, db, uid, key, company_id=company_id)
    data = {
        "name": s, "amount": float(amount or 0.0), "amount_type": amount_type,
        "type_tax_use": "sale", "tax_group_id": int(_coerce_id(grp_id)),
        "invoice_repartition_line_ids": [(0,0,{"repartition_type":"base","factor_percent":100}),
                                         (0,0,{"repartition_type":"tax","factor_percent":100})],
        "refund_repartition_line_ids":  [(0,0,{"repartition_type":"base","factor_percent":100}),
                                         (0,0,{"repartition_type":"tax","factor_percent":100})],
        "company_id": int(company_id) if company_id else False,
    }
    tid = retry(models.execute_kw, db, uid, key, "account.tax", "create",
                [[data]], {"context": company_ctx(company_id)})
    CACHE.tax_by_name[k] = (tid, amount_type)
    return tid

def get_or_create_percent_tax(models, db, uid, key, percent, company_id=None, preferred_name=None):
    global CACHE
    if not hasattr(CACHE, "tax_percent_by_amount"):
        CACHE.tax_percent_by_amount = {}
    k = (float(percent), int(company_id or 0))
    if k in CACHE.tax_percent_by_amount:
        return CACHE.tax_percent_by_amount[k]
    if preferred_name:
        tid = retry(models.execute_kw, db, uid, key, "account.tax", "search",
                    [[("name", "=", preferred_name), ("amount_type", "=", "percent")]],
                    {"limit": 1, "context": company_ctx(company_id)})
        if tid:
            CACHE.tax_percent_by_amount[k] = tid[0]; return tid[0]
    tids = retry(models.execute_kw, db, uid, key, "account.tax", "search",
                 [[("amount", "=", float(percent)), ("amount_type", "=", "percent")]],
                 {"limit": 1, "context": company_ctx(company_id)})
    if tids:
        CACHE.tax_percent_by_amount[k] = tids[0]; return tids[0]
    name = preferred_name or (f"VAT {int(percent)}%" if float(percent).is_integer() else f"VAT {percent}%")
    t = get_or_create_tax(models, db, uid, key, name, company_id=company_id, amount=float(percent), amount_type="percent")
    CACHE.tax_percent_by_amount[k] = t
    return t

def get_or_create_category(models, db, uid, key, path, company_id=None, model_name="product.category"):
    global CACHE
    if not path: return False
    raw = str(path).replace("\\", "/")
    pieces = [p.strip() for p in raw.split("/") if p and p.strip()]
    if not pieces: return False

    norm_key = (model_name, _norm(raw), int(company_id or 0))
    if norm_key in CACHE.categories:
        return CACHE.categories[norm_key]

    parent_id = False
    for seg in pieces:
        dom = [("name", "=", seg)]
        if parent_id: dom.append(("parent_id", "=", int(_coerce_id(parent_id))))
        ids = retry(models.execute_kw, db, uid, key, model_name, "search",
                    [dom], {"limit": 1, "context": company_ctx(company_id)})
        if ids:
            parent_id = ids[0]; continue
        vals = {"name": seg, "parent_id": int(_coerce_id(parent_id)) if parent_id else False}
        if model_name == "product.category" and company_id and model_has_field(models, db, uid, key, "product.category", "company_id"):
            vals["company_id"] = int(_coerce_id(company_id))
        try:
            parent_id = retry(models.execute_kw, db, uid, key, model_name, "create",
                              [[vals]], {"context": company_ctx(company_id)})
        except xmlrpc.client.Fault:
            ids2 = retry(models.execute_kw, db, uid, key, model_name, "search",
                         [dom], {"limit": 1, "context": company_ctx(company_id)})
            if ids2: parent_id = ids2[0]
            else: raise
    CACHE.categories[norm_key] = parent_id
    return parent_id

def get_or_create_supplier(models, db, uid, key, supplier_name, company_id=None):
    global CACHE
    name = str(supplier_name or "").strip()
    if not name: return None
    k = (_norm(name), int(company_id or 0))
    if k in CACHE.partner_by_name:
        return CACHE.partner_by_name[k]
    dom = [("name", "=", name), ("supplier_rank", ">", 0)]
    ids = retry(models.execute_kw, db, uid, key, "res.partner", "search",
                [dom], {"limit": 1, "context": company_ctx(company_id)})
    if ids:
        CACHE.partner_by_name[k] = ids[0]; return ids[0]
    nid = retry(models.execute_kw, db, uid, key, "res.partner", "create",
                [[{"name": name, "supplier_rank": 1}]], {"context": company_ctx(company_id)})
    CACHE.partner_by_name[k] = nid
    return nid

# -----------------------------------------------------------------------------
# Warehouse / locatie / put-away
# -----------------------------------------------------------------------------
def _find_wh_by_code(models, db, uid, key, code, company_id=None):
    global CACHE
    k = (_norm(code), int(company_id or 0))
    if k in CACHE.wh_code_id: return CACHE.wh_code_id[k]
    dom = [("code", "=", str(code).strip())]
    if company_id and model_has_field(models, db, uid, key, "stock.warehouse", "company_id"):
        dom.append(("company_id", "in", [int(company_id), False]))
    ids = retry(models.execute_kw, db, uid, key, "stock.warehouse", "search", [dom], {"limit": 1})
    wh = ids and ids[0] or None
    CACHE.wh_code_id[k] = wh
    return wh

def _read_wh_roots(models, db, uid, key, wh_id):
    global CACHE
    if wh_id in CACHE.wh_roots: return CACHE.wh_roots[wh_id]
    data = retry(models.execute_kw, db, uid, key, "stock.warehouse", "read",
                 [[int(wh_id)], ["lot_stock_id", "wh_input_stock_loc_id", "wh_output_stock_loc_id", "view_location_id","code"]])[0]
    roots = {
        "stock": data.get("lot_stock_id") and data["lot_stock_id"][0],
        "input": data.get("wh_input_stock_loc_id") and data["wh_input_stock_loc_id"][0],
        "output": data.get("wh_output_stock_loc_id") and data["wh_output_stock_loc_id"][0],
        "view": data.get("view_location_id") and data["view_location_id"][0],
        "code": data.get("code") or "WH",
    }
    CACHE.wh_roots[wh_id] = roots
    return roots

def _get_default_warehouse_id(models, db, uid, key, company_id=None):
    wh = _find_wh_by_code(models, db, uid, key, "WH", company_id=company_id)
    if wh: return wh
    dom = []
    if company_id and model_has_field(models, db, uid, key, "stock.warehouse", "company_id"):
        dom = [("company_id", "in", [int(company_id), False])]
    ids = retry(models.execute_kw, db, uid, key, "stock.warehouse", "search", [dom], {"limit": 1})
    return ids and ids[0] or None

def get_or_create_location_by_path(models, db, uid, key, path, company_id=None, create_missing=True):
    global CACHE
    if not path: return None
    raw = str(path).strip().replace("\\", "/")
    parts = [p for p in (seg.strip() for seg in raw.split("/")) if p]
    if len(parts) == 0: return None

    kn = (_norm(raw), int(company_id or 0))
    if kn in CACHE.loc_path: return CACHE.loc_path[kn]

    wh_code = parts[0]
    wh_id = _find_wh_by_code(models, db, uid, key, wh_code, company_id=company_id)
    if not wh_id:
        raise ValueError(f"Warehouse met code '{wh_code}' niet gevonden.")

    roots = _read_wh_roots(models, db, uid, key, wh_id)
    view_root  = roots.get("view")
    stock_root = roots.get("stock")
    current_parent = view_root or stock_root

    i = 1
    if i < len(parts):
        alias = parts[i].lower()
        if alias in ("stock", "voorraad"):
            current_parent = stock_root or current_parent; i += 1
        elif alias == "input":
            current_parent = roots.get("input") or current_parent; i += 1
        elif alias == "output":
            current_parent = roots.get("output") or current_parent; i += 1
        else:
            current_parent = stock_root or current_parent

    for j in range(i, len(parts)):
        seg = parts[j]
        dom = [("name", "=", seg), ("location_id", "=", int(_coerce_id(current_parent)))]
        if company_id and model_has_field(models, db, uid, key, "stock.location", "company_id"):
            dom.append(("company_id", "in", [int(company_id), False]))
        ids = retry(models.execute_kw, db, uid, key, "stock.location", "search", [dom], {"limit": 1})
        if ids:
            current_parent = ids[0]; continue

        dom = [("name", "ilike", seg), ("location_id", "=", int(_coerce_id(current_parent)))]
        if company_id and model_has_field(models, db, uid, key, "stock.location", "company_id"):
            dom.append(("company_id", "in", [int(company_id), False]))
        ids = retry(models.execute_kw, db, uid, key, "stock.location", "search", [dom], {"limit": 1})
        if ids:
            current_parent = ids[0]; continue

        if not create_missing: return None

        is_leaf = (j == len(parts) - 1)
        vals = {
            "name": seg, "location_id": int(_coerce_id(current_parent)),
            "usage": "internal" if is_leaf else "view", "active": True,
        }
        if company_id and model_has_field(models, db, uid, key, "stock.location", "company_id"):
            vals["company_id"] = int(company_id)

        current_parent = retry(models.execute_kw, db, uid, key, "stock.location", "create", [[vals]])

    loc_id = int(_coerce_id(current_parent))
    CACHE.loc_path[kn] = loc_id
    return loc_id

def _detect_putaway_fields(models, db, uid, key):
    meta = _fields_get_cached(models, db, uid, key, "stock.putaway.rule")
    prod_field = "product_id" if ("product_id" in meta and meta["product_id"].get("relation") == "product.product") else (
        "product_tmpl_id" if ("product_tmpl_id" in meta and meta["product_tmpl_id"].get("relation") in ("product.template","product.product")) else None
    )
    cand_apply = [f for f, m in meta.items() if m.get("type")=="many2one" and m.get("relation")=="stock.location"]
    apply_field = None
    for name in ["location_id", "location_in_id", "location_src_id"]:
        if name in cand_apply: apply_field = name; break
    if not apply_field and cand_apply:
        apply_field = cand_apply[0]
    dest_field = None
    for name in ["putaway_location_id", "location_dest_id", "fixed_location_id"]:
        if name in meta and meta[name].get("relation")=="stock.location":
            dest_field = name; break
    if not dest_field:
        for f in cand_apply:
            if f != apply_field:
                dest_field = f; break
    return prod_field, apply_field, dest_field

# -----------------------------------------------------------------------------
# Image helpers
# -----------------------------------------------------------------------------
def _best_image_field_for_product(models, db, uid, key):
    meta = _fields_get_cached(models, db, uid, key, "product.template")
    for f in ("image_1920", "image_1024", "image"):
        if f in meta and meta[f].get("type") == "binary":
            return f
    return "image_1920"

def _best_image_field_for_gallery(models, db, uid, key):
    meta = _fields_get_cached(models, db, uid, key, "product.image")
    for f in ("image_1920", "image_1024", "image"):
        if f in meta and meta[f].get("type") == "binary":
            return f
    return "image_1920"

def _filename_from_url(url: str) -> str:
    try:
        tail = url.split("?")[0].rstrip("/").split("/")[-1]
        return tail or "image"
    except Exception:
        return "image"

def _download_and_prepare_image(url: str, max_px: int = None):
    url = _normalize_url(url)
    global CACHE
    key = _norm(url)
    if key in CACHE.image_by_url:
        return CACHE.image_by_url[key]

    max_px = max_px or MAX_IMG_PX
    with SESSION_HTTP.get(url, timeout=(5, 30), stream=True) as resp:
        resp.raise_for_status()
        content = resp.content

    if len(content) > MAX_IMG_BYTES:
        raise Exception(f"image too large: {len(content)} bytes")
    name = _filename_from_url(url)

    with Image.open(BytesIO(content)) as im:
        if im.mode in ("RGBA", "LA"):
            bg = Image.new("RGB", im.size, (255, 255, 255))
            bg.paste(im, mask=im.split()[-1])
            im = bg
        else:
            im = im.convert("RGB")
        w, h = im.size
        scale = min(1.0, float(max_px) / float(max(w, h)))
        if scale < 1.0:
            im = im.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
        out = BytesIO()
        im.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        out.seek(0)
        b64 = base64.b64encode(out.read()).decode("ascii")
        fname = name if name.lower().endswith((".jpg",".jpeg")) else name + ".jpg"
        CACHE.image_by_url[key] = (b64, "image/jpeg", fname)
        return CACHE.image_by_url[key]

def _set_main_image(models, db, uid, key, product_id, company_id, url):
    b64, _mimetype, _fname = _download_and_prepare_image(url)
    field = _best_image_field_for_product(models, db, uid, key)
    retry(models.execute_kw, db, uid, key, "product.template", "write",
          [ensure_ids_list(product_id), {field: b64}],
          {"context": company_ctx(company_id)})

def _ensure_gallery_image(models, db, uid, key, product_tmpl_id, company_id, url):
    b64, mimetype, fname = _download_and_prepare_image(url)
    try:
        retry(models.execute_kw, db, uid, key, "product.image", "fields_get", [], {"attributes":["type"]})
        img_field = _best_image_field_for_gallery(models, db, uid, key)
        vals = {"name": fname, "product_tmpl_id": int(_coerce_id(product_tmpl_id)), img_field: b64, "active": True}
        retry(models.execute_kw, db, uid, key, "product.image", "create", [[vals]], {"context": company_ctx(company_id)})
        return
    except Exception:
        pass
    try:
        vals = {"name": fname, "res_model": "product.template", "res_id": int(_coerce_id(product_tmpl_id)),
                "type": "binary", "mimetype": mimetype, "datas": b64}
        retry(models.execute_kw, db, uid, key, "ir.attachment", "create", [[vals]], {"context": company_ctx(company_id)})
    except Exception as e:
        logging.warning(f"Kon attachment niet maken voor {product_tmpl_id}: {e}")

def _process_one_image(models, db, uid, key, kind, product_id, company_id, url):
    if kind == "main":
        _set_main_image(models, db, uid, key, product_id, company_id, url)
    else:
        _ensure_gallery_image(models, db, uid, key, product_id, company_id, url)

# -----------------------------------------------------------------------------
# Document helpers (2x URL)
# -----------------------------------------------------------------------------
def _publish_attachment_public(models, db, uid, key, attach_id, company_id=None):
    """Zet ir.attachment publiek (en indien aanwezig website_published op True)."""
    try:
        # Altijd public = True voor toegang via web
        retry(models.execute_kw, db, uid, key, "ir.attachment", "write",
              [[int(attach_id)], {"public": True}],
              {"context": company_ctx(company_id)})

        # Sommige databases hebben ook 'website_published' op attachment
        try:
            fget = retry(models.execute_kw, db, uid, key, "ir.attachment", "fields_get", [],
                         {"attributes": ["type"]})
            if "website_published" in fget:
                retry(models.execute_kw, db, uid, key, "ir.attachment", "write",
                      [[int(attach_id)], {"website_published": True}],
                      {"context": company_ctx(company_id)})
        except Exception:
            pass
    except Exception:
        pass

def _publish_document_on_website(models, db, uid, key, doc_id, company_id=None):
    doc_id = int(_coerce_id(doc_id))
    try:
        fget = retry(models.execute_kw, db, uid, key, "document.document", "fields_get", [],
                     {"attributes": ["type"]})
    except Exception:
        fget = {}
    vals = {}
    if "website_published" in fget:
        vals["website_published"] = True
    if "active" in fget:
        vals["active"] = True
    if vals:
        try:
            retry(models.execute_kw, db, uid, key, "document.document", "write",
                  [[doc_id], vals], {"context": company_ctx(company_id)})
        except Exception:
            pass

def link_document_to_product_page(models, db, uid, key, product_tmpl_id, doc_id, attach_id=None, company_id=None):
    """
    Zet 'shown_on_product_page' via het relatiemodel achter product_document_ids.
    Werkt in Odoo 19 varianten: relation kan koppelen via document_id *of* attachment_id,
    en inverse kan product_tmpl_id *of* product_id zijn.
    """
    product_tmpl_id = int(_coerce_id(product_tmpl_id))
    if doc_id is not None:
        doc_id = int(_coerce_id(doc_id))
    if attach_id is not None:
        attach_id = int(_coerce_id(attach_id))

    # 1) Metadata op product.template
    p_fields = retry(models.execute_kw, db, uid, key, "product.template", "fields_get", [],
                     {"attributes": ["type", "relation", "relation_field"]})
    fmeta = p_fields.get("product_document_ids") or p_fields.get("product_document_id")
    if not fmeta or fmeta.get("type") not in ("one2many", "many2many"):
        return

    rel_model = fmeta.get("relation")
    if not rel_model:
        return

    # 2) Relatiemodel bekijken
    rel_fields = retry(models.execute_kw, db, uid, key, rel_model, "fields_get", [],
                       {"attributes": ["type", "relation"]})

    # Inverse veld naar product
    inv_name = fmeta.get("relation_field") or fmeta.get("inverse_name") or "product_tmpl_id"
    if inv_name not in rel_fields:
        # fallback: kies wat er is
        if "product_tmpl_id" in rel_fields:
            inv_name = "product_tmpl_id"
        elif "product_id" in rel_fields:
            inv_name = "product_id"

    # Linkveld naar document of attachment
    doc_link_field = None
    for cand in ("document_id", "doc_id", "document"):
        if cand in rel_fields and rel_fields[cand].get("type") == "many2one" and rel_fields[cand].get("relation") == "document.document":
            doc_link_field = cand
            break
    uses_attachment = False
    if not doc_link_field:
        if "attachment_id" in rel_fields and rel_fields["attachment_id"].get("type") == "many2one" and rel_fields["attachment_id"].get("relation") == "ir.attachment":
            uses_attachment = True
        else:
            # laatste gok: eender welk m2o naar document.document
            for fname, meta in rel_fields.items():
                if meta.get("type") == "many2one" and meta.get("relation") == "document.document":
                    doc_link_field = fname
                    break

    # Boolean veld
    shown_field = None
    for cand in ("shown_on_product_page", "show_on_product_page", "is_shown_on_product_page"):
        if cand in rel_fields and rel_fields[cand].get("type") == "boolean":
            shown_field = cand
            break
    if not shown_field:
        return

    # 3) Mogelijke inverse value (template of variant)
    inv_value = int(_coerce_id(product_tmpl_id))
    inv_alt_field = None
    inv_alt_value = None
    if inv_name == "product_id":
        # we hebben een variant nodig
        prod_ids = retry(models.execute_kw, db, uid, key, "product.product", "search",
                         [[("product_tmpl_id", "=", int(product_tmpl_id))]], {"limit": 1})
        if prod_ids:
            inv_value = int(_coerce_id(prod_ids[0]))
    elif "product_id" in rel_fields:
        # prepareer een alternatieve inverse voor het geval inverse anders is
        prod_ids = retry(models.execute_kw, db, uid, key, "product.product", "search",
                         [[("product_tmpl_id", "=", int(product_tmpl_id))]], {"limit": 1})
        if prod_ids:
            inv_alt_field = "product_id"
            inv_alt_value = int(_coerce_id(prod_ids[0]))

    # 4) Zoeken of er al een rel-record bestaat
    dom = [(inv_name, "=", inv_value)]
    if doc_link_field:
        dom.append((doc_link_field, "=", int(doc_id)))
    elif uses_attachment and attach_id:
        dom.append(("attachment_id", "=", int(attach_id)))

    rel_id = None
    try:
        found = retry(models.execute_kw, db, uid, key, rel_model, "search", [dom],
                      {"limit": 1, "context": company_ctx(company_id)})
        if found:
            rel_id = int(found[0])
    except Exception:
        rel_id = None

    # 5) Upsert + zet boolean
    if rel_id:
        retry(models.execute_kw, db, uid, key, rel_model, "write",
              [[rel_id], {shown_field: True}],
              {"context": company_ctx(company_id)})
    else:
        vals = {shown_field: True}
        vals[inv_name] = inv_value
        if doc_link_field:
            vals[doc_link_field] = int(doc_id)
        elif uses_attachment and attach_id:
            vals["attachment_id"] = int(attach_id)

        # Als inverse onverwacht is, probeer een 2de create met alternatief
        try:
            retry(models.execute_kw, db, uid, key, rel_model, "create",
                  [[vals]], {"context": company_ctx(company_id)})
        except Exception:
            if inv_alt_field and inv_alt_value and inv_alt_field in rel_fields:
                vals.pop(inv_name, None)
                vals[inv_alt_field] = inv_alt_value
                retry(models.execute_kw, db, uid, key, rel_model, "create",
                      [[vals]], {"context": company_ctx(company_id)})

    # Forceer zichtbaarheid/naam op het relatiemodel
    try:
        rf = retry(models.execute_kw, db, uid, key, rel_model, "fields_get", [], {"attributes": ["type"]})
    except Exception:
        rf = {}
    vals_force = {}
    if "shown_on_product_page" in rf:
        vals_force["shown_on_product_page"] = True
    if "website_published" in rf:
        vals_force["website_published"] = True
    if vals_force:
        retry(models.execute_kw, db, uid, key, rel_model, "write",
              [[int(_coerce_id(rel_id))], vals_force],
              {"context": company_ctx(company_id)})


def model_exists(models, db, uid, key, model_name):
    try:
        retry(models.execute_kw, db, uid, key, model_name, 'fields_get', [], {'attributes': ['string'], 'limit': 1})
        return True
    except Exception:
        return False

def ensure_url_attachment_for_product(models, db, uid, key, tmpl_id, company_id, url, title=None, public=True):
    url = _normalize_url(url)
    if not url:
        return None
    name = (title or url).strip()[:255]

    dom = [
        ('res_model', '=', 'product.template'),
        ('res_id', '=', int(_coerce_id(tmpl_id))),
        ('type', '=', 'url'),
        ('url', '=', str(url).strip()),
    ]
    att_ids = retry(models.execute_kw, db, uid, key, 'ir.attachment', 'search', [dom],
                    {'limit': 1, 'context': company_ctx(company_id)})
    if att_ids:
        vals = {}
        if name: vals['name'] = name
        if public is not None and model_has_field(models, db, uid, key, 'ir.attachment', 'public'):
            vals['public'] = bool(public)
        if vals:
            retry(models.execute_kw, db, uid, key, 'ir.attachment', 'write', [att_ids, vals],
                  {'context': company_ctx(company_id)})
        att_id = _safe_int(att_ids[0])
    else:
        vals = {
            'name': name,
            'type': 'url',
            'url': str(url).strip(),
            'res_model': 'product.template',
            'res_id': int(_coerce_id(tmpl_id)),
        }
        if public is not None and model_has_field(models, db, uid, key, 'ir.attachment', 'public'):
            vals['public'] = bool(public)
        att_id = retry(models.execute_kw, db, uid, key, 'ir.attachment', 'create', [[vals]],
                       {'context': company_ctx(company_id)})
        att_id = _safe_int(att_id)

    # Publiceren van de attachment zelf (best effort)
    if att_id:
        _publish_attachment_public(models, db, uid, key, att_id, company_id=company_id)

    # ★ NIEUW: ook een document.document voorzien & publiceren (URL-variant)
    try:
        if model_exists(models, db, uid, key, 'document.document'):
            existing_doc = retry(models.execute_kw, db, uid, key, "document.document", "search",
                                 [[("attachment_id", "=", int(att_id)),
                                   ("res_model", "=", "product.template"),
                                   ("res_id", "=", int(_coerce_id(tmpl_id)))]],
                                 {"limit": 1, "context": company_ctx(company_id)})
            if existing_doc:
                doc_id = _safe_int(existing_doc[0])
                retry(models.execute_kw, db, uid, key, "document.document", "write",
                      [[int(doc_id)], {"website_published": True, "active": True, "name": name}],
                      {"context": company_ctx(company_id)})
            else:
                doc_vals = {
                    "name": name,
                    "attachment_id": int(att_id),
                    "res_model": "product.template",
                    "res_id": int(_coerce_id(tmpl_id)),
                    "website_published": True,
                    "active": True,
                }
                doc_id = retry(models.execute_kw, db, uid, key, "document.document", "create",
                               [[doc_vals]], {"context": company_ctx(company_id)})
                doc_id = _safe_int(doc_id)

            if doc_id:
                _publish_document_on_website(models, db, uid, key, doc_id, company_id=company_id)
    except Exception as e:
        logging.warning(f"Kon document.document niet publiceren voor attachment {att_id}: {e}")

    return att_id

def _ensure_scheme(u: str) -> str:
    if not u:
        return u
    s = str(u).strip()
    if not re.match(r'^https?://', s, flags=re.I):
        return "https://" + s
    return s

def _safe_int(x):
    try:
        if isinstance(x, (list, tuple)):
            return _safe_int(x[0])
        return int(float(str(x)))
    except Exception:
        return None

def link_attachment_via_product_document(models, db, uid, key,
                                         tmpl_id, attachment_id, company_id,
                                         title=None, show_on_product_page=True,
                                         log_fn=None):
    """
    Upsert product.document voor (product_tmpl_id, attachment_id) met uitgebreide logs:
    - name vullen (verplicht),
    - shown_on_product_page exact zetten (True/False),
    - website_published (indien veld bestaat) op True,
    - eindcontrole met read-back.
    """
    def L(msg): 
        try:
            (log_fn or (lambda m: logger.info(m)))(f"🧪 DOC: {msg}")
        except Exception:
            logger.info(msg)

    if not attachment_id:
        L("⛔ Geen attachment_id ontvangen — stoppen.")
        return None

    tmpl_id = _safe_int(tmpl_id)
    attachment_id = _safe_int(attachment_id)
    if not tmpl_id or not attachment_id:
        L(f"⛔ Ongeldige IDs: tmpl_id={tmpl_id} attachment_id={attachment_id}")
        return None

    if not model_exists(models, db, uid, key, 'product.document'):
        L("⛔ Model 'product.document' bestaat niet in deze database.")
        return None

    # fields_get
    try:
        fget = retry(models.execute_kw, db, uid, key, "product.document", "fields_get", [],
                     {"attributes": ["type"]})
        L(f"fields_get(product.document) OK; velden: {', '.join(sorted(fget.keys()))[:200]}…")
    except Exception as e:
        fget = {}
        L(f"⚠️ fields_get(product.document) faalde: {e}")

    # Naam bepalen (verplicht in veel databases)
    safe_name = None
    if title and str(title).strip():
        safe_name = str(title).strip()
    else:
        try:
            att_rec = retry(models.execute_kw, db, uid, key, "ir.attachment", "read",
                            [[attachment_id], ["name"]],
                            {"context": company_ctx(company_id)})
            safe_name = (att_rec and (att_rec[0].get("name") or "")) or None
        except Exception as e:
            L(f"⚠️ attachment read voor name faalde: {e}")
            safe_name = None
    if not safe_name:
        safe_name = f"Document voor product {tmpl_id}"
    L(f"titel/safe_name = {safe_name!r}")

    # Zoek bestaand product.document
    dom = [('product_tmpl_id', '=', int(tmpl_id)), ('attachment_id', '=', int(attachment_id))]
    try:
        pd_ids = retry(models.execute_kw, db, uid, key, 'product.document', 'search', [dom],
                       {'limit': 1, 'context': company_ctx(company_id)})
        L(f"search(product.document, dom={dom}) -> {pd_ids}")
    except Exception as e:
        L(f"⛔ search(product.document) fout: {e}")
        pd_ids = []

    # Schrijfwaarden opbouwen
    vals = {}
    if "name" in fget:
        vals["name"] = safe_name
    if "shown_on_product_page" in fget:
        vals["shown_on_product_page"] = bool(show_on_product_page)
    else:
        L("⚠️ Veld 'shown_on_product_page' bestaat NIET op product.document!")
    if "website_published" in fget:
        vals["website_published"] = True

    # Upsert
    pd_id = None
    try:
        if pd_ids:
            pd_id = _safe_int(pd_ids[0])
            L(f"write(product.document,{pd_id}, {vals})")
            retry(models.execute_kw, db, uid, key, "product.document", "write",
                  [[pd_id], vals], {"context": company_ctx(company_id)})
        else:
            create_vals = {'product_tmpl_id': int(tmpl_id), 'attachment_id': int(attachment_id)}
            create_vals.update(vals)
            L(f"create(product.document, {create_vals})")
            pd_id = retry(models.execute_kw, db, uid, key, 'product.document', 'create', [[create_vals]],
                          {'context': company_ctx(company_id)})
            pd_id = _safe_int(pd_id)
    except Exception as e:
        L(f"⛔ write/create product.document fout: {e}")
        return None

    # Read-back verificatie
    try:
        fields_to_read = ["name", "attachment_id", "product_tmpl_id"]
        if "shown_on_product_page" in fget: fields_to_read.append("shown_on_product_page")
        if "website_published" in fget: fields_to_read.append("website_published")
        rec = retry(models.execute_kw, db, uid, key, "product.document", "read",
                    [[int(pd_id)], fields_to_read], {"context": company_ctx(company_id)})
        L(f"read-back product.document[{pd_id}] -> {rec}")
    except Exception as e:
        L(f"⚠️ read-back product.document fout: {e}")

    return pd_id

def link_document_via_rel_auto(models, db, uid, key,
                               tmpl_id, attachment_id, company_id,
                               title=None, show_on_product_page=True,
                               log_fn=None):
    """
    Koppelt een attachment aan de productpagina via het *echte* relationele model
    achter product.template.product_document_ids en zet zichtbaarheid exact.
    Alles met defensieve _safe_int() + uitgebreide logs zodat we *zien* waar het stuk gaat.
    """
    def L(msg):
        try:
            (log_fn or (lambda m: logger.info(m)))(f"🧪 DOC-AUTO2: {msg}")
        except Exception:
            logger.info(msg)

    tmpl_id = _safe_int(tmpl_id)
    att_id  = _safe_int(attachment_id)
    L(f"start tmpl_id={tmpl_id!r} att_id={att_id!r} show={bool(show_on_product_page)} title={title!r}")

    if not tmpl_id or not att_id:
        L(f"⛔ Ongeldige IDs → tmpl_id={tmpl_id!r}, att_id={att_id!r}")
        return None

    # 1) metadata product.template
    try:
        p_fields = retry(models.execute_kw, db, uid, key, "product.template", "fields_get", [],
                         {"attributes": ["type","relation","relation_field","inverse_name"]})
    except Exception as e:
        L(f"⛔ fields_get(product.template) fout: {e}")
        return None

    fmeta = p_fields.get("product_document_ids") or p_fields.get("product_document_id")
    if not fmeta or fmeta.get("type") not in ("one2many","many2many"):
        L("⛔ Geen product_document_ids veld — stoppen")
        return None

    rel_model = fmeta.get("relation")
    inv_name  = fmeta.get("relation_field") or fmeta.get("inverse_name") or "product_tmpl_id"
    if not rel_model:
        L("⛔ Geen relation model op product_document_ids")
        return None
    L(f"rel_model={rel_model} inv_name={inv_name}")

    # 2) velden op koppelmodel
    try:
        r_fields = retry(models.execute_kw, db, uid, key, rel_model, "fields_get", [],
                         {"attributes": ["type","relation","string"]})
    except Exception as e:
        L(f"⛔ fields_get({rel_model}) fout: {e}")
        return None

    # linkveld detectie
    link_field = None
    link_is_attachment = False
    if "attachment_id" in r_fields and r_fields["attachment_id"].get("type")=="many2one" and r_fields["attachment_id"].get("relation")=="ir.attachment":
        link_field = "attachment_id"; link_is_attachment = True
    elif "document_id" in r_fields and r_fields["document_id"].get("type")=="many2one" and r_fields["document_id"].get("relation")=="document.document":
        link_field = "document_id"; link_is_attachment = False
    else:
        # fallback zoeken
        for fname, meta in r_fields.items():
            if meta.get("type")=="many2one" and meta.get("relation")=="ir.attachment":
                link_field = fname; link_is_attachment = True; break
        if not link_field:
            for fname, meta in r_fields.items():
                if meta.get("type")=="many2one" and meta.get("relation")=="document.document":
                    link_field = fname; link_is_attachment = False; break

    shown_field = None
    for cand in ("shown_on_product_page","show_on_product_page","is_shown_on_product_page"):
        if cand in r_fields and r_fields[cand].get("type") == "boolean":
            shown_field = cand; break

    has_name = "name" in r_fields and r_fields["name"].get("type") == "char"
    L(f"link_field={link_field} (via_attachment={link_is_attachment}) shown_field={shown_field} has_name={has_name}")

    if not link_field or not shown_field:
        L("⛔ Geen bruikbaar linkveld of geen zichtbaarheidsveld → stoppen")
        return None

    # veilige titel ophalen
    safe_name = None
    if has_name:
        if title and str(title).strip():
            safe_name = str(title).strip()
        else:
            try:
                att_rec = retry(models.execute_kw, db, uid, key, "ir.attachment", "read",
                                [[int(_safe_int(att_id))], ["name"]],
                                {"context": company_ctx(company_id)})
                safe_name = (att_rec and (att_rec[0].get("name") or "")) or None
            except Exception as e:
                L(f"⚠️ attachment read voor name faalde: {e}")
        if not safe_name:
            safe_name = f"Document voor product {tmpl_id}"

    # 3) bestaan check
    dom = [(inv_name, "=", int(_safe_int(tmpl_id)))]
    if link_is_attachment:
        dom.append((link_field, "=", int(_safe_int(att_id))))
    L(f"search({rel_model}, dom={dom})")
    try:
        existing = retry(models.execute_kw, db, uid, key, rel_model, "search", [dom],
                         {"limit": 1, "context": company_ctx(company_id)})
    except Exception as e:
        L(f"⛔ search({rel_model}) fout: {e}")
        existing = []

    vals = {inv_name: int(_safe_int(tmpl_id)), shown_field: bool(show_on_product_page)}
    if link_is_attachment:
        vals[link_field] = int(_safe_int(att_id))
    if has_name:
        vals["name"] = safe_name

    try:
        if existing:
            rel_id = _safe_int(existing[0])
            L(f"write({rel_model}, id={rel_id}, vals={vals})")
            retry(models.execute_kw, db, uid, key, rel_model, "write",
                  [[int(rel_id)], vals], {"context": company_ctx(company_id)})
        else:
            L(f"create({rel_model}, vals={vals})")
            rel_id = retry(models.execute_kw, db, uid, key, rel_model, "create", [[vals]],
                           {"context": company_ctx(company_id)})
            rel_id = _safe_int(rel_id)
    except Exception as e:
        L(f"⛔ write/create op {rel_model} fout: {e}")
        return None

    # Read-back ter verificatie
    try:
        fields_to_read = [inv_name, link_field, shown_field]
        if has_name: fields_to_read.append("name")
        rec = retry(models.execute_kw, db, uid, key, rel_model, "read",
                    [[int(_safe_int(rel_id))], fields_to_read], {"context": company_ctx(company_id)})
        L(f"read-back {rel_model}[{rel_id}] -> {rec}")
    except Exception as e:
        L(f"⚠️ read-back {rel_model} fout: {e}")

    return rel_id

def inject_website_link(html_current, url, title):
    if not url:
        return html_current or ''
    safe_title = (title or 'Bekijk document').strip()
    snippet = (
        f'<p><a href="{url}" target="_blank" rel="noopener" '
        f'style="display:inline-block;padding:8px 12px;border-radius:8px;'
        f'background:#1d4ed8;color:white;text-decoration:none">'
        f'{safe_title}</a></p>'
    )
    html_current = (html_current or '').strip()
    return (snippet + "\n" + html_current) if html_current else snippet

# -----------------------------------------------------------------------------
# PDF helpers (download & attach as binary)
# -----------------------------------------------------------------------------
MAX_PDF_BYTES = int(os.environ.get("MAX_PDF_BYTES", str(25 * 1024 * 1024)))  # 25MB

def _filename_from_pdf_url(url: str) -> str:
    try:
        tail = url.split("?")[0].rstrip("/").split("/")[-1]
        if not tail:
            return "document.pdf"
        if not re.search(r"\.pdf$", tail, flags=re.I):
            return tail + ".pdf"
        return tail
    except Exception:
        return "document.pdf"

def _download_pdf(url: str) -> tuple[str, str, str]:
    """
    Download een PDF en retourneer (base64_datas, mimetype, filename).
    Gooit Exception bij fout of bij te groot bestand.
    """
    with SESSION_HTTP.get(url, timeout=(8, 60), stream=True) as resp:
        resp.raise_for_status()

        # Limiteer grootte tijdens streamen
        buf = BytesIO()
        total = 0
        for chunk in resp.iter_content(chunk_size=16384):
            if not chunk:
                continue
            buf.write(chunk)
            total += len(chunk)
            if total > MAX_PDF_BYTES:
                raise Exception(f"PDF te groot: {total} bytes (max {MAX_PDF_BYTES})")
        content = buf.getvalue()

    # Basic content-type check (niet hard falen als server foutief headert)
    ctype = resp.headers.get("Content-Type", "").lower()
    mimetype = "application/pdf"
    if "pdf" not in ctype and not content.startswith(b"%PDF"):
        # fallback: toch proberen, maar markeer als pdf
        pass

    name = _filename_from_pdf_url(url)
    b64 = base64.b64encode(content).decode("ascii")
    return b64, mimetype, name

def _attach_pdf_to_product(models, db, uid, key, product_tmpl_id, company_id, url, title=None, make_public=True):
    """
    Maakt/overschrijft een ir.attachment bij product.template met de binair gedownloade PDF.
    Vermijdt dubbele bijlagen met zelfde naam+model+res_id.
    """
    url = _normalize_url(url)
    b64, mimetype, fname = _download_pdf(url)
    if title:
        # Gebruik titel als bestandsnaam-basis
        base = re.sub(r"[^\w\-\.\(\)\[\] ]+", "_", str(title).strip()) or "document"
        if not base.lower().endswith(".pdf"):
            base += ".pdf"
        fname = base

    dom = [
        ("res_model", "=", "product.template"),
        ("res_id", "=", int(_coerce_id(product_tmpl_id))),
        ("name", "=", fname),
        ("mimetype", "=", "application/pdf"),
    ]
    existing = retry(models.execute_kw, db, uid, key, "ir.attachment", "search", [dom], {"limit": 1})
    vals = {
        "name": fname,
        "res_model": "product.template",
        "res_id": int(_coerce_id(product_tmpl_id)),
        "type": "binary",
        "mimetype": mimetype,
        "datas": b64,
        "public": True,  # ← ZET DIRECT PUBLIEK
    }
    
    ctx = {"context": company_ctx(company_id)}
    # 1) Maak de ir.attachment (linkt aan het product)
    if existing:
        retry(models.execute_kw, db, uid, key, "ir.attachment", "write", [[existing[0]], vals], ctx)
        att_id = _safe_int(existing[0])
    else:
        att_id = retry(models.execute_kw, db, uid, key, "ir.attachment", "create", [[vals]], ctx)
        att_id = _safe_int(att_id)

    # Zet (best effort) ook website_published op attachment als veld bestaat
    if make_public:
        _publish_attachment_public(models, db, uid, key, att_id, company_id=company_id)

    # 2) Maak of vind het document.document dat deze attachment aan het product koppelt
    doc_id = None
    try:
        # Eerst zoeken of er al een document is voor deze attachment
        existing_doc = retry(models.execute_kw, db, uid, key, "document.document", "search",
                         [[("attachment_id", "=", int(att_id)),
                           ("res_model", "=", "product.template"),
                           ("res_id", "=", int(_coerce_id(product_tmpl_id)))]],
                         {"limit": 1, "context": company_ctx(company_id)})
        if existing_doc:
            doc_id = _safe_int(existing_doc[0])
    except Exception:
        doc_id = None

    if not doc_id:
        doc_vals = {
            "name": fname,
            "attachment_id": int(att_id),
            "res_model": "product.template",
            "res_id": int(_coerce_id(product_tmpl_id)),
        }
        try:
            doc_id = retry(models.execute_kw, db, uid, key, "document.document", "create",
                           [doc_vals], {"context": company_ctx(company_id)})
            doc_id = _safe_int(doc_id)
        except Exception:
            doc_id = None

    # 3) Publiceer ALLEEN het document op website (toggle “zichtbaar op website”)
    if doc_id and make_public:
        _publish_document_on_website(models, db, uid, key, doc_id, company_id=company_id)

    # 4) Koppel het document aan het product via product_document_ids (Odoo 19+)
    if doc_id:
        try:
            link_document_to_product_page(
                models, db, uid, key,
                product_tmpl_id=int(_coerce_id(product_tmpl_id)),
                doc_id=int(_coerce_id(doc_id)),
                attach_id=int(_coerce_id(att_id)) if att_id else None,
                company_id=company_id
            )
        except Exception as e:
            logging.warning(f"link_document_to_product_page failed (tmpl={product_tmpl_id}, doc={doc_id}): {e}")
            # geen raise, we gaan verder

    try:
        # Link tonen op productpagina met titel
        link_attachment_via_product_document(
            models, db, uid, key,
            tmpl_id=product_tmpl_id,
            attachment_id=att_id,
            company_id=company_id,
            title=title or fname,
            show_on_product_page=True
        )
    except Exception:
        pass

    return att_id



# -----------------------------------------------------------------------------
# UI mapping groepen
# -----------------------------------------------------------------------------
FIELD_GROUPS_BASE = {
    "Algemeen": [
        ("name", "Naam (standaard)"),
        ("default_code", "Interne Referentie"),
        ("barcode", "Barcode"),
        ("is_storable", "Voorraad bijhouden? (ja/nee)"),
        ("__PRODUCT_TYPE__", "Product Type"),
        ("categ_id", "Categorie"),
        ("categ_id:path", "Categorie (Pad-notatie)"),
        ("uom_id", "Verkoop UoM"),
        ("uom_po_id", "Aankoop UoM"),
        ("description", "Interne Omschrijving"),
        ("weight", "Gewicht"),
        ("product_tag_ids", "Tags (komma)"),
        ("public_categ_ids", "Website Categorieën"),
        ("public_categ_ids:path", "Website Categorieën (Pad-notatie)"),
        ("pos_categ_ids", "POS Categorieën"),
        ("pos_categ_ids:path", "POS Categorieën (Pad-notatie)"),
    ],
    "Verkoop": [
        ("list_price", "Verkoopprijs"),
        ("taxes_id", "BTW/Taksen (namen, komma)"),
        ("sale_ok", "Verkoopbaar (True/False)"),
        ("available_in_pos", "Beschikbaar in POS (True/False)"),
        ("is_published", "Gepubliceerd (True/False)"),
        ("invoice_policy", "Facturatiebeleid (bestelde/geleverde)"),
        ("route_ids", "Routes (Buy, MTO, …)"),
    ],
    "Aankoop / Leverancier": [
        ("purchase_ok", "Aankoopbaar (True/False)"),
        ("standard_price", "Kostprijs"),
        ("supplier", "Leverancier Naam (virtueel)"),
        ("supplier_product_code", "Leverancier Productcode (virtueel)"),
        ("aankoopprijs", "Inkoopprijs (virtueel)"),
        ("min_order_qty", "Minimum Bestelhoeveelheid (virtueel)"),
        ("levertijd", "Levertijd (dagen) (virtueel)"),
    ],
    "Content (standaardtaal)": [
        ("description_sale", "Verkoopomschrijving (standaard)"),
        ("website_description", "Website Omschrijving (standaard)"),
        ("website_meta_title", "SEO Titel"),
        ("website_meta_description", "SEO Omschrijving"),
                # Documents (virtueel UI-veld)
        ("document_url_1", "Document 1 URL (PDF/datasheet)"),
        ("document_title_1", "Document 1 Titel"),
        ("document_show_on_website_1", "Toon Document 1 op website? (True/False)"),
        ("document_url_2", "Document 2 URL (PDF/datasheet)"),
        ("document_title_2", "Document 2 Titel"),
        ("document_show_on_website_2", "Toon Document 2 op website? (True/False)"),
        ("product_document_ids/shown_on_product_page", "Toon document(en) op productpagina (True/False)"),

    ],
    "Inventaris": [
        ("tracking", "Tracering (none/lot/serial)"),
        ("responsible_id", "Verantwoordelijke"),
        ("stock_quantity", "Voorraadhoeveelheid (virtueel)"),
        ("inventory_location_path", "Locatiepad (bv. WH/Stock/PA1)"),
        ("inventory_putaway_code", "Put-away code (bv. 1F3D5)"),
    ],
    "Media": [
        ("image_url", "Afbeelding (URL) — hoofd"),
        ("image_urls", "Extra afbeeldingen (URL’s, komma/; gescheiden)"),
    ],
    "Documenten": [
        ("pdf_url_1", "Productdocument PDF — URL 1"),
        ("pdf_url_2", "Productdocument PDF — URL 2"),
    ],
    "Boekhouding": [
        ("property_account_income_id", "Opbrengstenrekening"),
        ("property_account_expense_id", "Kostenrekening"),
    ],
    "Heffingen": [
        ("RECUPEL", "Recupel (vast bedrag)"),
        ("BEBAT", "Bebat (vast bedrag)"),
    ],
}

def _build_clean_grouped_fields(models, db, uid, key):
    try:
        all_fields = retry(models.execute_kw, db, uid, key, "product.template", "fields_get", [],
                           {"attributes":["string","type","selection"]})
    except Exception:
        all_fields = {}
    ptype_field = "detailed_type" if "detailed_type" in all_fields else ("type" if "type" in all_fields else None)
    grouped, seen = [], set()
    for grp, items in FIELD_GROUPS_BASE.items():
        present=[]
        for fname, label in items:
            if fname == "__PRODUCT_TYPE__":
                if ptype_field and ptype_field in all_fields and ptype_field not in seen:
                    lab = all_fields[ptype_field].get("string") or label
                    present.append({"key": ptype_field, "label": lab}); seen.add(ptype_field)
                continue

            s = label
            if fname in TRANSLATABLE_FIELDS and "(standaard)" not in s:
                s = f"{s} (standaard)"
            
            # Toon veld als het bestaat in Odoo OF als het een virtueel veld is
            # Virtuele velden zijn o.a.: supplier, image_url, document_url_*, pdf_url_*, etc.
            if fname not in seen:
                present.append({"key": fname, "label": s}); seen.add(fname)
        if present: grouped.append({"group": grp, "fields": present})


    try:
        langs = get_active_languages(models, db, uid, key)
        trans=[]
        label_map={"name":"Naam","description_sale":"Verkoopomschrijving","website_description":"Website Omschrijving"}
        default_lang = get_default_lang(models, db, uid, key)
        for base in TRANSLATABLE_FIELDS:
            for code, _nm in langs:
                if code==default_lang: continue
                key_ = f"{base}[{code}]"
                if key_ in seen: continue
                trans.append({"key": key_, "label": f"{label_map.get(base, base)} ({code})"}); seen.add(key_)
        if trans: grouped.insert(0, {"group":"Vertalingen","fields": trans})
    except Exception:
        pass

    try:
        dynamic=[]
        for fname, meta in all_fields.items():
            if fname in seen: continue
            if fname in ("id","create_uid","create_date","write_uid","write_date",
                         "message_follower_ids","message_partner_ids","message_ids",
                         "activity_ids","activity_type_id","activity_state"):
                continue
            dynamic.append({"key": fname, "label": meta.get("string") or fname}); seen.add(fname)
        if dynamic:
            dynamic.sort(key=lambda x:(x["label"] or "").lower())
            grouped.append({"group":"Alle velden","fields": dynamic})
    except Exception:
        pass
    return grouped

def product_type_field_and_selection(models, db, uid, key):
    meta = _fields_get_cached(models, db, uid, key, "product.template")
    if "detailed_type" in meta and meta["detailed_type"].get("type") == "selection":
        return "detailed_type", meta["detailed_type"].get("selection") or []
    if "type" in meta and meta["type"].get("type") == "selection":
        return "type", meta["type"].get("selection") or []
    return None, []

def coerce_user_type_value(selection, raw_value):
    if not selection or raw_value in (None, ""):
        return None
    val = str(raw_value).strip().lower()
    for key, label in selection:
        if str(key).lower() == val: return key
    for key, label in selection:
        if str(label or "").strip().lower() == val: return key
    for key, label in selection:
        if str(label or "").strip().lower().startswith(val): return key
    for key, alias_set in DETAILED_TYPE_ALIASES.items():
        if val in alias_set:
            for sk, _lab in selection:
                if str(sk) == key: return sk
    return None

# -----------------------------------------------------------------------------
# Prefetch helpers (bulk)
# -----------------------------------------------------------------------------
def _chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf; buf = []
    if buf: yield buf

def prefetch_existing_products(models, db, uid, key, names, barcodes, company_id):
    global CACHE
    ctx = {"context": company_ctx(company_id)}
    uniq_barcodes = [b for b in {str(b).strip() for b in barcodes if b}]
    for chunk in _chunked(uniq_barcodes, LOOKUP_CHUNK):
        dom = [["barcode", "in", chunk]]
        recs = retry(models.execute_kw, db, uid, key, "product.template", "search_read",
                     [dom], {"fields": ["id","barcode"], "limit": len(chunk), **ctx})
        for r in recs or []:
            bc = (r.get("barcode") or "").strip()
            if bc: CACHE.barcode_to_id[bc] = int(_coerce_id(r["id"]))
    uniq_names = [n for n in {str(n).strip() for n in names if n}]
    for chunk in _chunked(uniq_names, LOOKUP_CHUNK):
        dom = [["name", "in", chunk]]
        recs = retry(models.execute_kw, db, uid, key, "product.template", "search_read",
                     [dom], {"fields": ["id","name"], "limit": len(chunk), **ctx})
        for r in recs or []:
            nm = (r.get("name") or "").strip().lower()
            if nm: CACHE.name_to_id[nm] = int(_coerce_id(r["id"]))

def prefetch_taxes(models, db, uid, key, tax_names, company_id):
    global CACHE
    wanted = [t for t in {str(x).strip() for x in tax_names if x}]
    for chunk in _chunked(wanted, LOOKUP_CHUNK):
        dom = [["name","in", chunk]]
        recs = retry(models.execute_kw, db, uid, key, "account.tax", "search_read",
                     [dom], {"fields":["id","name","amount_type"], "limit": len(chunk), "context": company_ctx(company_id)})
        for r in recs or []:
            nm = (r.get("name") or "").strip()
            if nm:
                CACHE.tax_by_name[(_norm(nm), int(company_id or 0))] = (int(_coerce_id(r["id"])), r.get("amount_type"))

def prefetch_suppliers(models, db, uid, key, supplier_names, company_id):
    global CACHE
    wanted = [t for t in {str(x).strip() for x in supplier_names if x}]
    if not wanted: return
    for chunk in _chunked(wanted, LOOKUP_CHUNK):
        dom = [["name","in", chunk], ["supplier_rank",">",0]]
        recs = retry(models.execute_kw, db, uid, key, "res.partner", "search_read",
                     [dom], {"fields":["id","name"], "limit": len(chunk), "context": company_ctx(company_id)})
        for r in recs or []:
            nm = (r.get("name") or "").strip()
            if nm: CACHE.partner_by_name[(_norm(nm), int(company_id or 0))] = int(_coerce_id(r["id"]))

# -----------------------------------------------------------------------------
# Processor
# -----------------------------------------------------------------------------
def pick_storable_selection_key(selection):
    """
    Kies een geldige selection-key voor 'storable/stockable' type,
    op basis van wat Odoo in jouw omgeving echt ondersteunt.
    """
    if not selection:
        return None
    want = coerce_user_type_value(selection, "product")
    if want is not None:
        return want
    for key, label in selection:
        k = str(key).lower()
        l = str(label or "").lower()
        if any(tok in k for tok in ("product", "stock", "storable")) or any(tok in l for tok in ("product", "stock", "storable")):
            return key
    return selection[0][0]

def process_excel_job(job_id, url, db, uid, key, file_path, sheet_name, mapping, options):
    """Kernprocessor met pandas + bulk-prefetch en batch-create/write — verbeterd."""
    global CACHE
    job = get_job(job_id)
    CACHE = RunCache()

    transport = RequestsTransport()
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", transport=transport)

    def log(msg): job.push(sse_format("log", msg))
    def check_cancel():
        if job.is_cancelled(): raise RuntimeError("Job cancelled door gebruiker")

    try:
        # Context & opties
        default_lang = get_default_lang(models, db, uid, key)
        chosen_company_id = options.get("chosen_company_id") or get_user_company_id(models, db, uid, key)
        base_lang = normalize_lang_code(options.get("base_lang") or default_lang)
        fast_mode = bool(options.get("fast_mode"))
        skip_images = bool(options.get("skip_images"))
        req_workers = int(options.get("img_workers") or MAX_IMAGE_WORKERS)
        img_workers = max(1, min(req_workers, IMAGE_POOL_MAX))

        user_flush_every_rows = int(options.get("flush_every_rows") or 0)
        user_create_chunk = int(options.get("create_chunk") or 0)
        effective_flush_rows = None if fast_mode else (user_flush_every_rows or int(os.environ.get("FLUSH_EVERY_ROWS", "3000")))
        effective_create_chunk = user_create_chunk or CREATE_CHUNK

        log("✅ Import gestart (pandas + bulk optimalisaties)…")
        log(f"• Company: {chosen_company_id or '-'}  • Basistaal: {base_lang}  • Fast mode: {fast_mode}  • Skip images: {skip_images}  "
            f"• FLUSH_EVERY_ROWS={'off' if fast_mode else (effective_flush_rows or 'off')}  • CREATE_CHUNK={effective_create_chunk}  "
            f"• IMAGE_POOL_MAX={IMAGE_POOL_MAX}  • img_workers={img_workers}")

        base_write_ctx = {"context": company_ctx(chosen_company_id, lang=base_lang)}

        # ---------------------------------------------------------
        # Excel lezen
        # ---------------------------------------------------------
        job.set_phase("Excel lezen", 0, 1); job.maybe_progress(True)
        t0 = time.time()
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        if sheet_name not in xls.sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' niet gevonden in {os.path.basename(file_path)}.")
        df = xls.parse(sheet_name=sheet_name, dtype=str, keep_default_na=False)
        job.set_phase("Excel lezen", 1, 1); job.maybe_progress(True)
        log(f"──── END Excel lezen (duur {time.time()-t0:.2f}s) ────")

        columns = list(df.columns)
        if not columns: raise ValueError("Geen kolommen gevonden in het werkblad.")

        total_rows = int(df.shape[0])
        job.set_progress(processed=0, total=total_rows)
        job.maybe_progress(True)

        try: os.remove(file_path)
        except Exception: pass

        # ---------------------------------------------------------
        # Prefetch & meta
        # ---------------------------------------------------------
        job.set_phase("Prefetch/Meta", 0, 1); t0 = time.time()
        try: UOM.load(models, db, uid, key)
        except Exception: pass
        _prefetch_routes(models, db, uid, key, company_id=chosen_company_id)
        default_wh_id = _get_default_warehouse_id(models, db, uid, key, company_id=chosen_company_id)
        if default_wh_id:
            wh_rec = retry(models.execute_kw, db, uid, key, "stock.warehouse", "read", [[int(default_wh_id)], ["code"]])
            default_wh_code = (wh_rec and wh_rec[0].get("code")) or "WH"
        else:
            default_wh_code = "WH"
        ptype_field, ptype_selection = product_type_field_and_selection(models, db, uid, key)
        log(f"ptype_field={ptype_field} selection={ptype_selection}")
        job.set_phase("Prefetch/Meta", 1, 1); job.maybe_progress(True)
        log(f"──── END Prefetch/Meta (duur {time.time()-t0:.2f}s) ────")

        # ---------------------------------------------------------
        # Header analyse: vertaalindices vooraf
        # ---------------------------------------------------------
        job.set_phase("Header analyse", 0, 1)
        header_translation = {}
        for col in columns:
            for rx in TRANSLATION_COL_REGEXES:
                m = rx.match(str(col))
                if m:
                    base_field = m.group(1)
                    lang_code = normalize_lang_code(m.group(2))
                    if base_field in TRANSLATABLE_FIELDS:
                        header_translation[col] = (base_field, lang_code)
                        break
        job.set_phase("Header analyse", 1, 1); job.maybe_progress(True)

        # ---------------------------------------------------------
        # Eerste scan voor prefetch
        # ---------------------------------------------------------
        job.set_phase("Scan voor prefetch", 0, 1); t0 = time.time()
        scan_names, scan_barcodes = [], []
        scan_taxes, scan_suppliers = set(), set()

        map_get = mapping.get
        for _, row in df.iterrows():
            for col in columns:
                field = map_get(col) or ""
                # Guard tegen undefined/null veldnamen
                field_str = str(field or "").strip()
                if field_str.lower() in ("", "undefined", "null"):
                    continue
                field = field_str
                
                raw = row[col]
                if raw == "": continue
                if field == "name":
                    scan_names.append(str(raw).strip())
                elif field == "barcode":
                    scan_barcodes.append(preserve_leading_zeros_str(raw))
                elif field == "taxes_id":
                    for tx in str(raw).split(","):
                        t = tx.strip()
                        if t: scan_taxes.add(t)
                elif field == "supplier":
                    scan_suppliers.add(str(raw).strip())

        prefetch_existing_products(models, db, uid, key, scan_names, scan_barcodes, chosen_company_id)
        prefetch_taxes(models, db, uid, key, scan_taxes, chosen_company_id)
        prefetch_suppliers(models, db, uid, key, scan_suppliers, chosen_company_id)
        job.set_phase("Scan voor prefetch", 1, 1); job.maybe_progress(True)
        log(f"──── END Scan voor prefetch (duur {time.time()-t0:.2f}s) ────")

        # ---------------------------------------------------------
        # Rijen verwerken → payloads
        # ---------------------------------------------------------
        job.set_phase("Rijen verwerken (payloads)", 0, total_rows)
        create_payloads = []   # (row_idx, vals_dict)
        rows_data = []
        image_jobs = []

        # Check voor generieke toggles in mapping
        has_doc_show_generic = any(v == "product_document_ids/shown_on_product_page" for v in mapping.values())
        has_doc_show_1 = any(v == "document_show_on_website_1" for v in mapping.values())
        has_doc_show_2 = any(v == "document_show_on_website_2" for v in mapping.values())

        processed = 0
        last_push_rows = 0

        for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
            check_cancel()
            processed += 1
            if effective_flush_rows:
                if processed - last_push_rows >= effective_flush_rows:
                    job.set_progress(processed=processed, total=total_rows)
                    job.set_phase("Rijen verwerken (payloads)", processed, total_rows)
                    job.maybe_progress(True); last_push_rows = processed
            else:
                if processed - last_push_rows >= PROGRESS_EVERY_ROWS:
                    job.set_progress(processed=processed, total=total_rows)
                    job.set_phase("Rijen verwerken (payloads)", processed, total_rows)
                    job.maybe_progress(); last_push_rows = processed

            row_dict = dict(zip(columns, row))

            base_vals, m2m_vals = {}, {}
            supplier_name = supplier_code = None
            buy_price = None
            min_qty = None
            delay = 0
            stock_qty = None
            desired_location_path = None
            putaway_code = None
            image_main_url = None
            image_extra_urls = []
            pdf_url_1 = None
            pdf_url_2 = None
            mapped_tax_ids, mapped_percent_ids = [], []
            bebat_id = None
            recupel_id = None
            translations_by_lang = {}
            
            # NIEUW: documentvelden
            document_url_1 = None
            document_title_1 = None
            document_show_on_website_1 = False

            document_url_2 = None
            document_title_2 = None
            document_show_on_website_2 = False
            
            show_docs_generic = False
            std_fields_explicit = set()

            # Vertalingen uit headers
            for col, ht in header_translation.items():
                if col not in row_dict: continue
                raw_val = row_dict[col]
                if raw_val == "": continue
                base_field, lang_code = ht
                translations_by_lang.setdefault(lang_code, {})[base_field] = str(raw_val)

            # Mappings
            for col in columns:
                raw = row_dict.get(col, "")
                field = map_get(col) or ""
                
                # Guard tegen undefined/null veldnamen
                field_str = str(field or "").strip()
                if field_str.lower() in ("", "undefined", "null"):
                    continue
                field = field_str
                
                if raw in (None, "") or not field: continue

                # mapping zelf is vertaling?
                translated_mapping = False
                for rx in TRANSLATION_COL_REGEXES:
                    mm = rx.match(str(field))
                    if mm:
                        base_field = mm.group(1)
                        lang_code = normalize_lang_code(mm.group(2))
                        if base_field in TRANSLATABLE_FIELDS:
                            translations_by_lang.setdefault(lang_code, {})[base_field] = str(raw)
                        translated_mapping = True
                        break
                if translated_mapping: continue

                if field == "supplier":
                    supplier_name = str(raw); continue
                if field == "supplier_product_code":
                    supplier_code = preserve_leading_zeros_str(raw); continue
                if field == "aankoopprijs":
                    d = parse_decimal(raw); buy_price = float(d) if d is not None else 0.0; continue
                if field == "min_order_qty":
                    try: min_qty = int(float(str(raw).replace(",", ".")))
                    except Exception: min_qty = 0
                    continue
                if field == "levertijd":
                    try: delay = int(float(str(raw).replace(",", ".")))
                    except Exception: delay = 0
                    continue
                if field == "stock_quantity":
                    val_str = "" if raw is None else str(raw).strip()
                    if not val_str or val_str.lower() in ("nan", "none", "-"):
                        stock_qty = None
                    else:
                        try: stock_qty = float(val_str.replace(",", "."))
                        except Exception: stock_qty = None
                    continue
                if field == "inventory_location_path":
                    desired_location_path = str(raw).strip(); continue
                if field == "inventory_putaway_code":
                    putaway_code = str(raw).strip(); continue
                if field == "image_url":
                    image_main_url = _normalize_url(str(raw).strip()); continue
                if field == "image_urls":
                    image_extra_urls = [_normalize_url(u.strip()) for u in re.split(r"[,\n;\|]", str(raw)) if u and u.strip()]
                    continue
                if field == "route_ids":
                    ids = _resolve_stock_route_ids(models, db, uid, key, raw, company_id=chosen_company_id)
                    if ids: m2m_vals["route_ids"] = [(6, 0, [int(_coerce_id(i)) for i in ids])]
                    continue
                if field == "is_storable":
                    if model_has_field(models, db, uid, key, "product.template", "is_storable"):
                        base_vals["is_storable"] = _coerce_bool(raw)
                    continue
                if field in ("type", "detailed_type"):
                    if ptype_field:
                        coerced = coerce_user_type_value(ptype_selection, raw)
                        if coerced: base_vals[ptype_field] = coerced
                    continue

                # NIEUW: documentvelden
                if field == "document_url_1":
                    document_url_1 = _normalize_url(str(raw).strip()); continue
                if field == "document_title_1":
                    document_title_1 = str(raw).strip(); continue
                if field == "document_show_on_website_1":
                    document_show_on_website_1 = _coerce_bool(raw); continue

                if field == "document_url_2":
                    document_url_2 = _normalize_url(str(raw).strip()); continue
                if field == "document_title_2":
                    document_title_2 = str(raw).strip(); continue
                if field == "document_show_on_website_2":
                    document_show_on_website_2 = _coerce_bool(raw); continue

                if field == "product_document_ids/shown_on_product_page":
                    show_docs_generic = _coerce_bool(raw)
                    continue

                if field == "pdf_url_1":
                    pdf_url_1 = _normalize_url(str(raw).strip()); continue
                if field == "pdf_url_2":
                    pdf_url_2 = _normalize_url(str(raw).strip()); continue

                # echte velden
                if field in TRANSLATABLE_FIELDS:
                    base_vals[field] = str(raw).strip(); std_fields_explicit.add(field)
                elif field == "categ_id":
                    base_vals["categ_id"] = get_or_create_category(models, db, uid, key, raw, company_id=chosen_company_id)
                elif field in ("public_categ_ids", "pos_categ_ids"):
                    mname = "product.public.category" if field == "public_categ_ids" else "pos.category"
                    ids = []
                    for piece in str(raw).split(","):
                        cid = get_or_create_category(models, db, uid, key, piece.strip(), company_id=chosen_company_id, model_name=mname)
                        if cid: ids.append(cid)
                    if ids: m2m_vals[field] = [(6, 0, [_coerce_id(i) for i in ids])]
                elif field == "product_tag_ids":
                    tag_ids = []
                    for t in str(raw).split(","):
                        tname = t.strip()
                        if not tname: continue
                        kn = _norm(tname)
                        if kn in CACHE.tag_by_name:
                            tag_ids.append(CACHE.tag_by_name[kn])
                        else:
                            tid = retry(models.execute_kw, db, uid, key, "product.tag", "search",
                                        [[("name", "=", tname)]], {"limit": 1})
                            if tid:
                                CACHE.tag_by_name[kn] = tid[0]; tag_ids.append(tid[0])
                            else:
                                new_tid = retry(models.execute_kw, db, uid, key, "product.tag", "create",
                                                [[{"name": tname}]])
                                CACHE.tag_by_name[kn] = new_tid; tag_ids.append(new_tid)
                    if tag_ids: m2m_vals["product_tag_ids"] = [(6, 0, [_coerce_id(i) for i in tag_ids])]
                elif field in ("uom_id", "uom_po_id"):
                    uom_id = UOM.get(models, db, uid, key, raw, company_ctx(chosen_company_id))
                    if uom_id: base_vals[field] = uom_id
                elif field in ("available_in_pos", "is_published", "sale_ok", "purchase_ok"):
                    base_vals[field] = _coerce_bool(raw)
                elif field == "taxes_id":
                    for tx in str(raw).split(","):
                        txn = tx.strip()
                        if not txn: continue
                        k = (_norm(txn), int(chosen_company_id or 0))
                        tid, amt_type = None, None
                        if k in CACHE.tax_by_name:
                            tid, amt_type = CACHE.tax_by_name[k]
                        else:
                            found = retry(models.execute_kw, db, uid, key, "account.tax", "search",
                                          [[("name", "=", txn)]],
                                          {"limit": 1, "context": company_ctx(chosen_company_id)})
                            if found:
                                tid = _coerce_id(found[0])
                                rec = retry(models.execute_kw, db, uid, key, "account.tax", "read",
                                            [[tid], ["amount_type"]],
                                            {"context": company_ctx(chosen_company_id)})
                                amt_type = (rec and rec[0].get("amount_type")) or None
                                CACHE.tax_by_name[k] = (tid, amt_type)
                        if tid:
                            if amt_type == "percent": mapped_percent_ids.append(_coerce_id(tid))
                            else: mapped_tax_ids.append(_coerce_id(tid))
                elif field == "RECUPEL":
                    d = parse_decimal(raw)
                    if d is not None and d > 0:
                        name = f"Recupel({format_decimal_for_name(d)})"
                        recupel_id = get_or_create_tax(models, db, uid, key, name, company_id=chosen_company_id, amount=float(d), amount_type="fixed")
                elif field == "BEBAT":
                    d = parse_decimal(raw)
                    if d is not None and d > 0:
                        name = f"Bebat({format_decimal_for_name(d)})"
                        bebat_id = get_or_create_tax(models, db, uid, key, name, company_id=chosen_company_id, amount=float(d), amount_type="fixed")
                elif field in ("property_account_income_id", "property_account_expense_id"):
                    if is_category_inherit(raw):
                        base_vals[field] = "__USE_CATEGORY__"
                    else:
                        kind = "income" if field == "property_account_income_id" else "expense"
                        acc_id = find_or_create_account(models, db, uid, key, raw, kind, company_id=chosen_company_id)
                        if acc_id: base_vals[field] = _coerce_id(acc_id)
                elif field == "invoice_policy":
                    s = str(raw or "").strip().lower()
                    if s:
                        if s.startswith("gelev") or "livr" in s:
                            base_vals["invoice_policy"] = "delivery"
                        elif s.startswith("bestel") or "command" in s:
                            base_vals["invoice_policy"] = "order"
                        else:
                            is_m2m, coerced = resolve_dynamic_field(models, db, uid, key, field, raw, chosen_company_id)
                            if coerced in ("order","delivery"):
                                base_vals["invoice_policy"] = coerced
                elif field == "barcode":
                    base_vals["barcode"] = preserve_leading_zeros_str(raw)
                else:
                    is_m2m, coerced = resolve_dynamic_field(models, db, uid, key, field, raw, chosen_company_id)
                    if is_m2m:
                        if coerced: m2m_vals[field] = [(6, 0, [int(_coerce_id(i)) for i in coerced])]
                    elif coerced is not None:
                        base_vals[field] = coerced

            # Fallbacks: als specifieke doc-show kolommen niet gemapt zijn, gebruik de generieke toggle
            if document_url_1 and not has_doc_show_1:
                document_show_on_website_1 = bool(show_docs_generic)
            if document_url_2 and not has_doc_show_2:
                document_show_on_website_2 = bool(show_docs_generic)

            # Dedupe basistaal
            if base_lang in translations_by_lang:
                for k in list(translations_by_lang[base_lang].keys()):
                    if k in TRANSLATABLE_FIELDS and (k in std_fields_explicit or k in base_vals):
                        translations_by_lang[base_lang].pop(k, None)
                if not translations_by_lang.get(base_lang):
                    translations_by_lang.pop(base_lang, None)

            # Lookup volgorde: 1) barcode, 2) naam
            product_id = None
            bc = (base_vals.get("barcode") or "").strip()
            nm = (base_vals.get("name") or "").strip()

            if bc and bc in CACHE.barcode_to_id:
                product_id = CACHE.barcode_to_id[bc]
            elif bc and bc.lstrip("0") in CACHE.barcode_to_id:
                product_id = CACHE.barcode_to_id[bc.lstrip("0")]
            if not product_id and nm:
                hit = CACHE.name_to_id.get(nm.lower())
                if hit: product_id = hit

            # fallback: als er géén name is maar we hebben supplier_code/barcode → genereer naam
            if not product_id and not nm:
                gen_name = None
                if supplier_name and supplier_code:
                    gen_name = f"{supplier_name.strip()} {supplier_code.strip()}"
                elif bc:
                    gen_name = f"ITEM-{bc}"
                elif supplier_code:
                    gen_name = f"ITEM-{supplier_code}"
                if gen_name:
                    base_vals.setdefault("name", gen_name)
                    nm = gen_name

            rows_data.append({
                "row_idx": idx, "base_vals": base_vals, "m2m_vals": m2m_vals,
                "supplier_name": supplier_name, "supplier_code": supplier_code,
                "buy_price": buy_price, "min_qty": min_qty, "delay": delay,
                "stock_qty": stock_qty, "desired_location_path": desired_location_path,
                "putaway_code": putaway_code, "image_main_url": image_main_url,
                "image_extra_urls": image_extra_urls,
                "pdf_url_1": pdf_url_1,
                "pdf_url_2": pdf_url_2,
                "mapped_tax_ids": mapped_tax_ids,
                "mapped_percent_ids": mapped_percent_ids, "bebat_id": bebat_id,
                "recupel_id": recupel_id, "translations_by_lang": translations_by_lang,
                "product_id": product_id,
                # NIEUW: documenten
                "document_url_1": document_url_1,
                "document_title_1": document_title_1,
                "document_show_on_website_1": document_show_on_website_1,
                "document_url_2": document_url_2,
                "document_title_2": document_title_2,
                "document_show_on_website_2": document_show_on_website_2,
            })

            # CREATE als niet gevonden (en nu moet er een naam zijn)
            if not product_id:
                if not base_vals.get("name"):
                    job.result_messages.append(f"Row {idx}: Fout: geen naam en kan geen naam genereren.")
                    log(f"❌ Row {idx}: geen naam — overgeslagen")
                    continue
                # detailed_type/stock hints + standaard: altijd 'storable' tenzij expliciet gemapt
                if ptype_field:
                    # 1) Standaard altijd storable als gebruiker niets invult
                    if not base_vals.get(ptype_field):
                        chosen = pick_storable_selection_key(ptype_selection)
                        if chosen is not None:
                            base_vals[ptype_field] = chosen

                    # 2) Indien signalen op voorraad (stock_qty/route/tracking), opnieuw afdwingen indien leeg
                    want_storable = (
                        bool(base_vals.get("is_storable")) or
                        (stock_qty is not None) or
                        bool(m2m_vals.get("route_ids")) or
                        (base_vals.get("tracking") in ("lot", "serial"))
                    )
                    if want_storable and not base_vals.get(ptype_field):
                        chosen = pick_storable_selection_key(ptype_selection)
                        if chosen is not None:
                            base_vals[ptype_field] = chosen
                vals = {k: v for k, v in base_vals.items() if v != "__USE_CATEGORY__"}
                
                # ... vlak voordat je vals in create_payloads stopt:
                if chosen_company_id and model_has_field(models, db, uid, key, "product.template", "company_id"):
                    base_vals.setdefault("company_id", int(_safe_int(chosen_company_id)))
                
                create_payloads.append((idx, vals))
                log(f"🏢 Effective company_id for create/write = {chosen_company_id!r}")

        job.set_phase("Rijen verwerken (payloads)", processed, total_rows)
        job.set_progress(processed=processed, total=total_rows); job.maybe_progress(True)

        # ---------------------------------------------------------
        # Batch-create voor nieuwe producten
        # ---------------------------------------------------------
        created_map = {}  # row_idx -> new_id
        if create_payloads:
            log(f"🧩 Aanmaken nieuwe producten in batches van {effective_create_chunk}…")
            job.set_phase("Creates", 0, len(create_payloads))
            done = 0
            for chunk in _chunked(create_payloads, effective_create_chunk):
                check_cancel()
                vals_list = [vals for (_i, vals) in chunk]
                ids = retry(models.execute_kw, db, uid, key, "product.template", "create", [vals_list], base_write_ctx)
                if isinstance(ids, int): ids = [ids]
                for (_i, _vals), new_id in zip(chunk, ids):
                    new_id = int(_coerce_id(new_id))
                    created_map[_i] = new_id
                    nm = (_vals.get("name") or "").strip()
                    bc = (_vals.get("barcode") or "").strip()
                    if nm: CACHE.name_to_id[nm.lower()] = new_id
                    if bc: CACHE.barcode_to_id[bc] = new_id
                done += len(chunk)
                job.set_phase("Creates", done, len(create_payloads))
                job.set_progress(processed=done, total=len(create_payloads)); job.maybe_progress()

        # ---------------------------------------------------------
        # Company cache
        # ---------------------------------------------------------
        all_product_ids = []
        for d in rows_data:
            pid = d["product_id"] or created_map.get(d["row_idx"])
            if pid: all_product_ids.append(int(_coerce_id(pid)))
        all_product_ids = list(dict.fromkeys(all_product_ids))

        product_company_cache = {}
        if chosen_company_id:
            for pid in all_product_ids:
                product_company_cache[pid] = int(_coerce_id(chosen_company_id))
        else:
            if all_product_ids:
                infos = retry(models.execute_kw, db, uid, key, "product.template", "read",
                              [all_product_ids, ["company_id"]], {"context": company_ctx(None)})
                for rec in infos or []:
                    cid = rec.get("company_id")
                    product_company_cache[int(_coerce_id(rec["id"]))] = int(_coerce_id(cid[0])) if cid else None

        # ---------------------------------------------------------
        # Verwerk Rijen in Chunks (Writes, Translations, Suppliers, Taxes, Put-away, Stock)
        # ---------------------------------------------------------
        # We splitsen de grote bak data op om geheugen te sparen en Odoo niet te overbelasten.
        job.set_phase("Bulkverwerking", 0, len(rows_data))
        
        # Images pool setup
        image_batch = []
        image_futures = []
        images_done_count = 0
        total_images_submitted = 0
        pool = ThreadPoolExecutor(max_workers=img_workers)

        def flush_images():
            nonlocal images_done_count, total_images_submitted, image_futures
            if not image_batch: return
            for (kind, pid, cid, url) in image_batch:
                fut = pool.submit(_process_one_image, models, db, uid, key, kind, pid, cid, url)
                image_futures.append(fut); total_images_submitted += 1
            image_batch.clear()

        # START CHUNK LOOP
        processed_total_chunks = 0
        chunk_size_process = 500

        for chunk_data in _chunked(rows_data, chunk_size_process):
            check_cancel()
            
            # --- 1. Pre-calc voor deze chunk (Locations, Variants) ---
            
            # A. Variant lookup (nodig voor stock/putaway)
            templates_needing_variant = []
            for d in chunk_data:
                pid = d["product_id"] or created_map.get(d["row_idx"])
                if not pid: continue
                if d["stock_qty"] is not None or d["putaway_code"]:
                    templates_needing_variant.append(int(_coerce_id(pid)))
            templates_needing_variant = list(dict.fromkeys(templates_needing_variant))

            tmpl_to_variant = {}
            if templates_needing_variant:
                for subchunk in _chunked(templates_needing_variant, LOOKUP_CHUNK):
                    check_cancel()
                    recs = retry(models.execute_kw, db, uid, key, "product.product", "search_read",
                                 [[("product_tmpl_id", "in", subchunk)]],
                                 {"fields": ["id", "product_tmpl_id"], "limit": len(subchunk)})
                    for r in recs or []:
                        tmpl_id = r.get("product_tmpl_id") and _coerce_id(r["product_tmpl_id"][0])
                        if tmpl_id:
                            tmpl_to_variant[int(tmpl_id)] = int(_coerce_id(r["id"]))

            # B. Location lookup (unieke paden in deze chunk)
            unique_paths = set()
            for d in chunk_data:
                if d["desired_location_path"]:
                    pid = d["product_id"] or created_map.get(d["row_idx"])
                    if pid:
                        comp_id = product_company_cache.get(int(_coerce_id(pid)))
                        unique_paths.add((comp_id, d["desired_location_path"]))

            path_to_loc = {}
            for comp_id, p in unique_paths:
                check_cancel()
                try:
                    loc_id = get_or_create_location_by_path(models, db, uid, key, p, company_id=comp_id, create_missing=True)
                    path_to_loc[(comp_id, p)] = loc_id
                except Exception:
                    path_to_loc[(comp_id, p)] = None

            # --- 2. Main Writes & Translations & Collection Loop ---
            putaway_jobs = []
            pdf_jobs = []

            for d in chunk_data:
                check_cancel()
                row_idx = d["row_idx"]
                pid = d["product_id"] or created_map.get(row_idx)
                if not pid: continue

                comp_id = product_company_cache.get(int(_coerce_id(pid)))
                ctx_base_lang = {"context": company_ctx(comp_id, lang=base_lang)}
                ctx_base = {"context": company_ctx(comp_id)}

                # Bouw combined values
                combined = {}
                for k, v in (d["base_vals"] or {}).items():
                    if v != "__USE_CATEGORY__": combined[k] = v
                for k, v in (d["m2m_vals"] or {}).items():
                    combined[k] = v
                for k in TRANSLATABLE_FIELDS:
                    v = d["base_vals"].get(k)
                    if v not in (None, "", "__USE_CATEGORY__"):
                        combined.setdefault(k, v)

                # Purchase ok flag
                if d["supplier_name"] and combined.get("purchase_ok") in (None, False):
                    combined["purchase_ok"] = True

                # Type enforcement
                if ptype_field:
                    if combined.get(ptype_field) in (None, ""):
                        chosen = pick_storable_selection_key(ptype_selection)
                        if chosen is not None: combined[ptype_field] = chosen
                    if combined.get(ptype_field) in (None, ""):
                        if (d["stock_qty"] is not None) or d["m2m_vals"].get("route_ids") or (combined.get("tracking") in ("lot","serial")):
                            chosen = pick_storable_selection_key(ptype_selection)
                            if chosen is not None: combined[ptype_field] = chosen

                # Write uitvoeren
                if combined:
                    try:
                        retry(models.execute_kw, db, uid, key, "product.template", "write",
                              [ensure_ids_list(pid), combined], ctx_base_lang)
                    except Exception as e:
                        log(f"❌ Fout bij schrijven product {pid} (rij {row_idx}): {e}")
                        continue

                # Company set fallback
                try:
                    if chosen_company_id and model_has_field(models, db, uid, key, "product.template", "company_id"):
                        rec = retry(models.execute_kw, db, uid, key, "product.template", "read",
                                    [[int(_coerce_id(pid))], ["company_id"]], {"context": company_ctx(comp_id)})
                        if not (rec and rec[0].get("company_id")):
                            retry(models.execute_kw, db, uid, key, "product.template", "write",
                                  [[int(_coerce_id(pid))], {"company_id": int(_coerce_id(chosen_company_id))}],
                                  {"context": company_ctx(comp_id)})
                except Exception as e:
                    log(f"⚠️ Company set (fallback) faalde voor product {pid}: {e}")

                # Accounting toggles
                try:
                    if d["base_vals"].get("property_account_income_id") == "__USE_CATEGORY__":
                        retry(models.execute_kw, db, uid, key, "product.template", "write",
                              [ensure_ids_list(pid), {"property_account_income_id": False}], ctx_base)
                    if d["base_vals"].get("property_account_expense_id") == "__USE_CATEGORY__":
                        retry(models.execute_kw, db, uid, key, "product.template", "write",
                              [ensure_ids_list(pid), {"property_account_expense_id": False}], ctx_base)
                except Exception as e:
                    log(f"⚠️ Fout bij accounting toggle (rij {row_idx}): {e}")

                # Translations
                try:
                    for lang_code, vals in (d["translations_by_lang"] or {}).items():
                        if lang_code == base_lang: continue
                        payload = {}
                        for k in TRANSLATABLE_FIELDS:
                            v = vals.get(k)
                            if v not in (None, ""): payload[k] = v
                        if payload:
                            retry(models.execute_kw, db, uid, key, "product.template", "write",
                                  [ensure_ids_list(pid), payload], {"context": company_ctx(comp_id, lang=lang_code)})
                except Exception as e:
                    log(f"⚠️ Fout bij vertalingen (rij {row_idx}): {e}")

                # Document URLs (DOC1/DOC2)
                try:
                    # Doc 1
                    if d.get("document_url_1"):
                        in_url, in_title, in_show = _ensure_scheme(d["document_url_1"]), d.get("document_title_1"), bool(d.get("document_show_on_website_1"))
                        att1 = ensure_url_attachment_for_product(models, db, uid, key, tmpl_id=int(_safe_int(pid)), company_id=comp_id, url=in_url, title=in_title, public=True)
                        if att1:
                            link_document_via_rel_auto(models, db, uid, key, tmpl_id=int(_safe_int(pid)), attachment_id=int(_safe_int(att1)), company_id=comp_id, title=in_title, show_on_product_page=in_show, log_fn=None)
                    # Doc 2
                    if d.get("document_url_2"):
                        in_url, in_title, in_show = _ensure_scheme(d["document_url_2"]), d.get("document_title_2"), bool(d.get("document_show_on_website_2"))
                        att2 = ensure_url_attachment_for_product(models, db, uid, key, tmpl_id=int(_safe_int(pid)), company_id=comp_id, url=in_url, title=in_title, public=True)
                        if att2:
                            link_document_via_rel_auto(models, db, uid, key, tmpl_id=int(_safe_int(pid)), attachment_id=int(_safe_int(att2)), company_id=comp_id, title=in_title, show_on_product_page=in_show, log_fn=None)
                except Exception as e:
                    log(f"⚠️ Document URLs verwerking fout (rij {row_idx}): {e}")

                # Images queue
                try:
                    if not skip_images:
                        if d["image_main_url"]:
                            image_batch.append(("main", int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None, d["image_main_url"]))
                        for u in d["image_extra_urls"]:
                            image_batch.append(("extra", int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None, u))
                        if len(image_batch) >= 50: flush_images()
                except Exception as e:
                    log(f"⚠️ Fout bij images klaarzetten (rij {row_idx}): {e}")

                # PDF Jobs collection
                try:
                    if d.get("pdf_url_1"):
                        pdf_jobs.append((int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None, d["pdf_url_1"], f"Productdocument 1 - {d['base_vals'].get('name') or ''}"))
                    if d.get("pdf_url_2"):
                        pdf_jobs.append((int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None, d["pdf_url_2"], f"Productdocument 2 - {d['base_vals'].get('name') or ''}"))
                except Exception as e:
                    log(f"⚠️ Fout bij PDF klaarzetten (rij {row_idx}): {e}")

                # Putaway jobs collection
                try:
                    if d["putaway_code"]:
                        putaway_jobs.append((int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None, default_wh_code, d["putaway_code"]))
                except Exception as e:
                    log(f"⚠️ Fout bij putaway klaarzetten (rij {row_idx}): {e}")

            # --- EINDE Loop over chunk_data (Writes/Prep) ---

            # Flush remaining images for this chunk
            flush_images()
            # Clean up done image futures to save memory
            active_futures = []
            for fut in image_futures:
                if fut.done():
                    try: fut.result()
                    except Exception as e:
                        job.result_messages.append(f"Afbeeldingstaak: {e}"); log(f"⚠️ Afbeeldingstaak: {e}")
                    images_done_count += 1
                else:
                    active_futures.append(fut)
            image_futures = active_futures            

            # --- 3. Process collected jobs for this chunk ---

            # PDF Documenten
            if pdf_jobs:
                for i, (tmpl_id, comp_id, url, title) in enumerate(pdf_jobs, start=1):
                    check_cancel()
                    if not url: continue
                    try:
                        _attach_pdf_to_product(models, db, uid, key, tmpl_id, comp_id, url, title=title, make_public=True)
                    except Exception as e:
                        job.result_messages.append(f"PDF niet toegevoegd ({url}): {e}")
                        log(f"⚠️ PDF niet toegevoegd ({url}): {e}")

            # Suppliers Info
            supplier_jobs = []
            for d in chunk_data:
                pid = d["product_id"] or created_map.get(d["row_idx"])
                if not pid or not d["supplier_name"]: continue
                comp_id = product_company_cache.get(int(_coerce_id(pid)))
                partner_id = get_or_create_supplier(models, db, uid, key, d["supplier_name"], company_id=comp_id)
                if not partner_id: continue
                price = float(d["buy_price"]) if d["buy_price"] is not None else 0.0
                supplier_jobs.append((int(_coerce_id(pid)), int(_coerce_id(comp_id)) if comp_id else None,
                                      int(_coerce_id(partner_id)), d["supplier_code"] or "", price, int(d["min_qty"] or 0), int(d.get("delay") or 0)))
            
            if supplier_jobs:
                tmpl_ids = sorted({j[0] for j in supplier_jobs})
                partner_ids = sorted({j[2] for j in supplier_jobs})
                existing = retry(models.execute_kw, db, uid, key, "product.supplierinfo", "search_read",
                                 [[("product_tmpl_id", "in", tmpl_ids), ("partner_id", "in", partner_ids)]],
                                 {"fields": ["id", "product_tmpl_id", "partner_id", "product_code"], "limit": MAX_XMLRPC_INT})
                sup_index = {}
                for r in existing or []:
                    t = _coerce_id(r["product_tmpl_id"][0]); p = _coerce_id(r["partner_id"][0]); code = (r.get("product_code") or "")
                    sup_index[(t, p, code)] = int(_coerce_id(r["id"]))
                
                to_write, to_create = [], []
                for (tmpl_id, comp_id, partner_id, product_code, price, mq, delay) in supplier_jobs:
                    check_cancel()
                    key_ = (tmpl_id, partner_id, product_code or "")
                    vals = {
                        "product_tmpl_id": tmpl_id, "partner_id": partner_id, "product_code": product_code or "",
                        "price": float(price), "min_qty": int(mq), "delay": int(delay),
                        "company_id": int(_coerce_id(comp_id)) if comp_id else False,
                    }
                    if key_ in sup_index:
                        to_write.append((sup_index[key_], {
                            "price": vals["price"], "min_qty": vals["min_qty"], "delay": vals["delay"], "product_code": vals["product_code"],
                        }))
                    else:
                        to_create.append(vals)

                for chunk in _chunked(to_write, 200):
                    for sid, v in chunk:
                         retry(models.execute_kw, db, uid, key, "product.supplierinfo", "write", [[sid], v], {"context": company_ctx(None)})

                for chunk in _chunked(to_create, 100):
                    retry(models.execute_kw, db, uid, key, "product.supplierinfo", "create", [chunk], {"context": company_ctx(None)})

            # Taxes
            buckets = defaultdict(list)
            for d in chunk_data:
                pid = d["product_id"] or created_map.get(d["row_idx"])
                if not pid: continue
                comp_id = product_company_cache.get(int(_coerce_id(pid)))
                
                mapped_tax_ids = list(d["mapped_tax_ids"])
                mapped_percent_ids = list(d["mapped_percent_ids"])
                
                # Check known taxes
                for t in list(mapped_tax_ids):
                    found = False
                    for val in CACHE.tax_by_name.values():
                        if val[0] == t: found = True; break
                    if not found:
                        rec = retry(models.execute_kw, db, uid, key, "account.tax", "read", [ensure_ids_list(t), ["amount_type"]], {"context": company_ctx(comp_id)})
                        amt_type = (rec and rec[0].get("amount_type")) or None
                        CACHE.tax_by_name[(f"id_{t}", int(comp_id or 0))] = (t, amt_type)
                        
                final_fixed_ids = set()
                for (tid, amt_type) in CACHE.tax_by_name.values():
                    if tid in mapped_tax_ids and amt_type != "percent": final_fixed_ids.add(_coerce_id(tid))
                if d["bebat_id"]: final_fixed_ids.add(_coerce_id(d["bebat_id"]))
                if d["recupel_id"]: final_fixed_ids.add(_coerce_id(d["recupel_id"]))
                
                if mapped_percent_ids:
                    final_percent = set(_coerce_id(x) for x in mapped_percent_ids)
                else:
                    vat21 = get_or_create_percent_tax(models, db, uid, key, 21.0, company_id=comp_id, preferred_name="VAT 21%")
                    final_percent = {_coerce_id(vat21)} if isinstance(vat21, int) else set()
                
                d["_final_taxes"] = tuple(sorted(set(final_fixed_ids).union(final_percent)))
                buckets[d["_final_taxes"]].append(int(_coerce_id(pid)))

            for tax_set, pid_list in buckets.items():
                check_cancel()
                try:
                    retry(models.execute_kw, db, uid, key, "product.template", "write", [pid_list, {"taxes_id": [(6, 0, list(tax_set))]}], {"context": company_ctx(chosen_company_id)})
                except Exception as e:
                    log(f"⚠️ Fout bij taxes apply in chunk: {e}")

            # Put-away Execution
            if putaway_jobs:
                for (tmpl_id, comp_id, wh_code, code) in putaway_jobs:
                    check_cancel()
                    try:
                        prod_field, apply_field, dest_field = _detect_putaway_fields(models, db, uid, key)
                        wh_id = _find_wh_by_code(models, db, uid, key, wh_code, company_id=comp_id) or _get_default_warehouse_id(models, db, uid, key, company_id=comp_id)
                        roots = _read_wh_roots(models, db, uid, key, wh_id)
                        stock_root = roots.get("stock")
                        if not stock_root: continue
                        
                        path = f"{roots.get('code')}/Stock/{str(code).strip()}"
                        dest_loc_id = get_or_create_location_by_path(models, db, uid, key, path, company_id=comp_id, create_missing=True)
                        
                        payload_filter = [(apply_field, "=", int(_coerce_id(stock_root)))]
                        payload_vals = {apply_field: int(_coerce_id(stock_root)), dest_field: int(_coerce_id(dest_loc_id))}
                        
                        if prod_field == "product_id":
                            variant_id = tmpl_to_variant.get(int(_coerce_id(tmpl_id)))
                            if not variant_id: continue
                            payload_filter.append((prod_field, "=", int(_coerce_id(variant_id))))
                            payload_vals[prod_field] = int(_coerce_id(variant_id))
                        else:
                            payload_filter.append(("product_tmpl_id", "=", int(_coerce_id(tmpl_id))))
                            payload_vals["product_tmpl_id"] = int(_coerce_id(tmpl_id))

                        rid = retry(models.execute_kw, db, uid, key, "stock.putaway.rule", "search", [payload_filter], {"limit": 1, "context": company_ctx(comp_id)})
                        if rid:
                            retry(models.execute_kw, db, uid, key, "stock.putaway.rule", "write", [ensure_ids_list(rid[0]), payload_vals], {"context": company_ctx(comp_id)})
                        else:
                            retry(models.execute_kw, db, uid, key, "stock.putaway.rule", "create", [[payload_vals]], {"context": company_ctx(comp_id)})
                    except Exception as e:
                        log(f"⚠️ Putaway fout: {e}")

            # Stock Updates
            stock_jobs = []
            for d in chunk_data:
                pid = d["product_id"] or created_map.get(d["row_idx"])
                if not pid or d["stock_qty"] is None: continue
                comp_id = product_company_cache.get(int(_coerce_id(pid)))
                variant_id = tmpl_to_variant.get(int(_coerce_id(pid)))
                if not variant_id: continue
                
                if d["desired_location_path"]:
                    loc_id = path_to_loc.get((comp_id, d["desired_location_path"]))
                else:
                    loc = retry(models.execute_kw, db, uid, key, "stock.location", "search", [[("usage","=","internal")]], {"limit":1, "context": company_ctx(comp_id)})
                    loc_id = _coerce_id(loc[0]) if loc else None
                
                if loc_id:
                    stock_jobs.append((int(_coerce_id(variant_id)), int(_coerce_id(comp_id)) if comp_id else None, int(_coerce_id(loc_id)), float(d["stock_qty"])))

            if stock_jobs:
                by_company = defaultdict(list)
                for item in stock_jobs: by_company[item[1]].append(item)
                
                for comp_id, items in by_company.items():
                    prod_ids = sorted({x[0] for x in items})
                    loc_ids = sorted({x[2] for x in items})
                    existing_quants = retry(models.execute_kw, db, uid, key, "stock.quant", "search_read",
                                            [[("product_id","in", prod_ids), ("location_id","in", loc_ids)]],
                                            {"fields": ["id","product_id","location_id"], "limit": MAX_XMLRPC_INT, "context": company_ctx(comp_id)})
                    quant_index = {}
                    for q in existing_quants or []:
                        quant_index[(_coerce_id(q["product_id"][0]), _coerce_id(q["location_id"][0]))] = int(_coerce_id(q["id"]))
                    
                    to_apply_ids, to_create = [], []
                    for (vid, cid, lid, qty) in items:
                        if (vid, lid) in quant_index:
                            qid = quant_index[(vid, lid)]
                            retry(models.execute_kw, db, uid, key, "stock.quant", "write", [[qid], {"inventory_quantity": qty}], {"context": company_ctx(comp_id)})
                            to_apply_ids.append(qid)
                        else:
                            to_create.append({"product_id": vid, "location_id": lid, "inventory_quantity": qty})
                    
                    if to_create:
                        for chunk in _chunked(to_create, 50):
                            new_ids = retry(models.execute_kw, db, uid, key, "stock.quant", "create", [chunk], {"context": company_ctx(comp_id)})
                            if isinstance(new_ids, int): new_ids = [new_ids]
                            to_apply_ids.extend([int(_coerce_id(x)) for x in new_ids])
                    
                    if to_apply_ids:
                        for chunk in _chunked(to_apply_ids, 200):
                            try:
                                retry(models.execute_kw, db, uid, key, "stock.quant", "action_apply_inventory", [chunk], {"context": company_ctx(comp_id)})
                            except Exception as e:
                                log(f"⚠️ Stock apply fout: {e}")

            # End of Chunk Progress
            processed_total_chunks += len(chunk_data)
            job.set_phase("Bulkverwerking", processed_total_chunks, len(rows_data))
            job.maybe_progress(True)
            
        job.maybe_progress(True)

        # ---------------------------------------------------------
        # Finalize images
        # ---------------------------------------------------------
        flush_images()
        for fut in as_completed(image_futures):
            check_cancel()
            try: fut.result()
            except Exception as e:
                job.result_messages.append(f"Afbeeldingstaak: {e}"); log(f"⚠️ Afbeeldingstaak: {e}")
            images_done_count += 1
            if total_images_submitted > 0:
                job.set_phase("Images (background)", images_done_count, total_images_submitted)
        pool.shutdown(wait=True)
        job.maybe_progress(True)

        # ---------------------------------------------------------
        # Klaar
        # ---------------------------------------------------------
        job.set_progress(processed=total_rows, total=total_rows)
        job.set_phase("Done", 1, 1)
        job.maybe_progress(True)
        log("✅ Klaar. Het eindrapport staat hieronder.")

    except Exception as e:
        job.error = str(e)
        log(f"❌ Jobfout: {e}")
    finally:
        job.mark_done()

# -----------------------------------------------------------------------------
# Flask routes
# -----------------------------------------------------------------------------
@app.route("/")
def home():
    if "uid" in session: return redirect(url_for("upload_excel"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        url = (request.form.get("url") or "").rstrip("/")
        db = request.form.get("db") or ""
        email = request.form.get("email") or ""
        api_key = request.form.get("api_key") or ""
        fast = request.form.get("fast") or request.args.get("fast") or ""
        session["fast_mode"] = str(fast).strip().lower() in ("1","true","yes","y","on")
        try:
            transport = RequestsTransport()
            common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", transport=transport)
            uid = common.authenticate(db, email, api_key, {})
            if not uid: return render_template("login.html", message="Invalid credentials")
            session.update({"url": url, "db": db, "email": email, "api_key": api_key, "uid": uid})
            return redirect(url_for("upload_excel"))
        except Exception as e:
            return render_template("login.html", message=f"Error: {e}")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/save_mapping", methods=["POST"])
def save_mapping():
    session["last_mapping"] = (request.json or {}).get("mapping") or {}
    session["last_settings"] = (request.json or {}).get("settings") or {}
    return jsonify({"ok": True})

@app.route("/load_mapping", methods=["GET"])
def load_mapping():
    return jsonify({
        "mapping": session.get("last_mapping", {}),
        "settings": session.get("last_settings", {})
    })

@app.route("/upload_excel", methods=["GET", "POST"])
def upload_excel():
    if "uid" not in session:
        return redirect(url_for("login"))

    if request.method == "GET":
        return render_template("excel_upload.html",
                               file_path="", sheets=[], sheet_name="", columns=None,
                               grouped_fields=[], example_row={},
                               langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE),
                               message=None)

    f = request.files.get("excel_file")
    if not f or not f.filename:
        return render_template("excel_upload.html", message="Geen bestand geselecteerd.",
                               file_path="", sheets=[], sheet_name="", columns=None, grouped_fields=[],
                               example_row={}, langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

    if not f.filename.lower().endswith(".xlsx"):
        return render_template("excel_upload.html", message="Upload enkel .xlsx bestanden.",
                               file_path="", sheets=[], sheet_name="", columns=None, grouped_fields=[],
                               example_row={}, langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

    safe = secure_filename(f.filename) or "upload.xlsx"
    unique = f"{uuid.uuid4().hex}_{safe}"
    file_path = os.path.join(UPLOADS, unique)
    try:
        f.save(file_path)
    except Exception as e:
        return render_template("excel_upload.html", message=f"Kon bestand niet opslaan: {e}",
                               file_path="", sheets=[], sheet_name="", columns=None, grouped_fields=[],
                               example_row={}, langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="", current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

    try:
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        sheets = xls.sheet_names
        first_sheet = sheets[0] if sheets else ""
    except Exception as e:
        return render_template("excel_upload.html", message=f"Excel kon niet gelezen worden: {e}",
                               file_path="", sheets=[], sheet_name="", columns=None, grouped_fields=[],
                               example_row={}, langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="", current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

    return render_mapping_page(file_path, sheets, first_sheet)

def render_mapping_page(file_path, sheets, sheet_name):
    try:
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        if sheet_name not in xls.sheet_names:
            sheet_name = xls.sheet_names[0] if xls.sheet_names else ""
        df_head = xls.parse(sheet_name=sheet_name, dtype=str, keep_default_na=False, nrows=1)
        columns = list(df_head.columns) if not df_head.empty else []
        example_row = (df_head.iloc[0].to_dict() if not df_head.empty else {})
    except Exception as e:
        return render_template("excel_upload.html",
                               message=f"Lezen van sheet '{sheet_name}' faalde: {e}",
                               file_path=file_path, sheets=sheets, sheet_name=sheet_name,
                               columns=None, grouped_fields=[], example_row={},
                               langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

    try:
        transport = RequestsTransport()
        models = xmlrpc.client.ServerProxy(f'{session["url"]}/xmlrpc/2/object', transport=transport)

        # 1) Groepen voor de linkerkant (select met 'Algemeen', 'Verkoop', …)
        grouped_fields = _build_clean_grouped_fields(models, session["db"], session["uid"], session["api_key"])

        # 2) Actieve talen + default
        langs = get_active_languages(models, session["db"], session["uid"], session["api_key"])
        default_lang = get_default_lang(models, session["db"], session["uid"], session["api_key"])

        # 3) Companies
        companies = get_companies(models, session["db"], session["uid"], session["api_key"])
        user_company_id = get_user_company_id(models, session["db"], session["uid"], session["api_key"])

        # 4) >>> Alle Odoo veldnamen + suffix-varianten voor autosuggest (rechts bij 'mapping')
        meta = retry(models.execute_kw, session["db"], session["uid"], session["api_key"],
                     "product.template", "fields_get", [],
                     {"attributes": ["type", "relation"]})

        base_names = sorted(meta.keys())

        # Voeg voor relationele velden de varianten :id, :name en :path toe
        relation_suffixes = []
        for fname, m in meta.items():
            ftype = (m or {}).get("type")
            if ftype in ("many2one", "many2many", "one2many"):
                relation_suffixes.append(f"{fname}:id")
                relation_suffixes.append(f"{fname}:name")
                # :path is zinvol voor modellen met parent_id-hiërarchie (publieke/pos/pt categorieën, eigen models)
                relation_suffixes.append(f"{fname}:path")

        # Jouw virtuele velden blijven bestaan
        VIRTUALS = [
            "supplier","supplier_product_code","aankoopprijs","min_order_qty","levertijd",
            "stock_quantity","RECUPEL","BEBAT","inventory_location_path","inventory_putaway_code",
            "image_url","image_urls","route_ids","is_storable",
            "document_url_1","document_title_1","document_show_on_website_1",
            "document_url_2","document_title_2","document_show_on_website_2",
            "pdf_url_1","pdf_url_2",
            # handige suffixes ook als suggestie
            "categ_id:path", "public_categ_ids:path", "pos_categ_ids:path"
        ]

        all_field_names = sorted(set(base_names + relation_suffixes + VIRTUALS))

    except Exception as e:
        return render_template("excel_upload.html",
                               message=f"Kan velden/talen/bedrijven niet laden: {e}",
                               file_path=file_path, sheets=sheets, sheet_name=sheet_name,
                               columns=columns, grouped_fields=[],
                               example_row=example_row, langs=[("en_US","English (US)")],
                               default_lang="en_US", companies=[],
                               selected_company_id="", current_fast=session.get("fast_mode", GLOBAL_FAST_MODE),
                               all_field_names=[])

    return render_template("excel_upload.html",
                           file_path=file_path, sheets=sheets, sheet_name=sheet_name,
                           columns=columns, grouped_fields=grouped_fields,
                           example_row=example_row, langs=langs, default_lang=default_lang,
                           companies=companies, selected_company_id=user_company_id,
                           current_fast=session.get("fast_mode", GLOBAL_FAST_MODE), message=None,
                           all_field_names=all_field_names)

@app.route("/select_sheet_excel", methods=["POST"])
def select_sheet_excel():
    if "uid" not in session:
        return redirect(url_for("login"))
    file_path = request.form.get("file_path") or session.get("last_upload_path") or ""
    if not file_path or not os.path.exists(file_path):
        return render_template("excel_upload.html",
                               message="Het geüploade Excel-bestand kon niet gevonden worden.",
                               file_path="", sheets=[], sheet_name="", columns=None,
                               grouped_fields=[], example_row={}, langs=[("en_US","English (US)")],
                               default_lang="en_US", companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))
    sheet = request.form.get("sheet") or ""
    try:
        xls = pd.ExcelFile(file_path, engine="openpyxl")
        if sheet not in xls.sheet_names:
            return render_template("excel_upload.html",
                                   message=f"Sheet '{sheet}' bestaat niet in {os.path.basename(file_path)}",
                                   file_path=file_path, sheets=xls.sheet_names, sheet_name=xls.sheet_names[0],
                                   columns=None, grouped_fields=[], example_row={}, langs=[("en_US","English (US)")],
                                   default_lang="en_US", companies=[], selected_company_id="",
                                   current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))
        sheets = xls.sheet_names
        return render_mapping_page(file_path, sheets, sheet)
    except Exception as e:
        return render_template("excel_upload.html",
                               message=f"Kon sheet niet openen: {e}",
                               file_path=file_path, sheets=[], sheet_name="",
                               columns=None, grouped_fields=[], example_row={},
                               langs=[("en_US","English (US)")], default_lang="en_US",
                               companies=[], selected_company_id="",
                               current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

@app.route("/start_process", methods=["POST"])
def start_process():
    if "uid" not in session:
        return jsonify({"error": "not_logged_in"}), 401

    url, db, uid, key = session["url"], session["db"], session["uid"], session["api_key"]
    file_path = request.form.get("file_path") or session.get("last_upload_path")
    sheet_name = request.form.get("sheet_name")

    try:
        chosen_company_id = int(request.form.get("company_id") or 0) or None
    except Exception:
        chosen_company_id = None

    fast_mode_ui = (request.form.get("fast_mode") in ("1","true","yes","on"))
    session["fast_mode"] = fast_mode_ui if fast_mode_ui is not None else session.get("fast_mode", GLOBAL_FAST_MODE)

    flush_every_rows = request.form.get("flush_every_rows")
    create_chunk = request.form.get("create_chunk")

    options = {
        "chosen_company_id": chosen_company_id,
        "base_lang": request.form.get("base_lang") or None,
        "fast_mode": bool(session.get("fast_mode", GLOBAL_FAST_MODE)),
        "skip_images": (request.form.get("skip_images") == "1"),
        "img_workers": int(request.form.get("img_workers") or MAX_IMAGE_WORKERS),
        "flush_every_rows": int(flush_every_rows) if (flush_every_rows and flush_every_rows.isdigit()) else None,
        "create_chunk": int(create_chunk) if (create_chunk and create_chunk.isdigit()) else None,
    }

    def _clean_val(s):
        s = (s or "").strip()
        if s.lower() in ("", "undefined", "null", "—", "-"):
            return ""
        return s

    mapping = {}
    for k, v in request.form.items():
        if not (k.startswith("mapping[") and k.endswith("]")):
            continue
        col = k[len("mapping["):-1]
        fld = _clean_val(v)
        col = _clean_val(col)
        if not col or not fld:
            continue
        mapping[col] = fld

    if not file_path:
        return jsonify({"error": "file_not_found"}), 400
    if not sheet_name:
        return jsonify({"error": "sheet_required"}), 400

    job_id = uuid.uuid4().hex
    job = JobState(job_id)
    JOBS[job_id] = job
    job.save()

    t = threading.Thread(
        target=process_excel_job,
        args=(job_id, url, db, uid, key, file_path, sheet_name, mapping, options),
        daemon=True
    )
    t.start()

    return jsonify({"job_id": job_id})

# -------------------------------
# NIEUWE, ROBUUSTE SSE ENDPOINT
# -------------------------------
@app.route("/logs/stream")
def logs_stream():
    job_id = request.args.get("job")
    job = get_job(job_id)
    if not job:
        return Response("event: log\ndata: Job niet gevonden\n\n", mimetype="text/event-stream")

    from flask import stream_with_context

    @stream_with_context
    def gen():
        keepalive_every = 15.0
        last_keep = time.time()

        # Eerste snapshot (client toont 'Verbonden…' bij 'open')
        yield sse_format("progress", {
            "processed": job.processed,
            "total": job.total,
            "phase": job.phase,
            "phase_processed": job.phase_processed,
            "phase_total": job.phase_total
        })

        try:
            while True:
                # Done + queue leeg ⇒ done uitsturen en sluiten
                if job.done and job.queue.empty():
                    yield sse_format("done", {"ok": job.error is None, "error": job.error})
                    return

                # Flush queued events non-blocking
                try:
                    item = job.queue.get_nowait()
                    if item == "__END__":
                        # done al gepusht in mark_done(); toch defensief:
                        yield sse_format("done", {"ok": job.error is None, "error": job.error})
                        return
                    yield item
                except Empty:
                    pass

                # Periodieke keepalive comment
                now = time.time()
                if now - last_keep >= keepalive_every:
                    last_keep = now
                    yield ": keepalive\n\n"

                time.sleep(0.2)  # CPU vriendelijk

        except (GeneratorExit, ClientDisconnected, ConnectionResetError, BrokenPipeError):
            # Client/WSGI sloot; niets meer schrijven
            return

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive",
    }
    return Response(gen(), headers=headers)

@app.route("/progress")
def progress():
    job_id = request.args.get("job")
    job = get_job(job_id)
    if not job:
        return jsonify({"processed": 0, "total": 1, "eta": 0, "done": True, "phase": None, "phase_processed": 0, "phase_total": 0})
    processed = job.processed
    total = job.total or 1
    elapsed = max(time.time() - job.start_time, 0.001)
    rate = processed / elapsed
    remaining = max(total - processed, 0)
    eta = (remaining / rate) if rate > 0 else 0
    return jsonify({
        "processed": processed, "total": total, "eta": eta, "done": job.done,
        "phase": job.phase, "phase_processed": job.phase_processed, "phase_total": job.phase_total
    })

@app.route("/final_messages")
def final_messages():
    job_id = request.args.get("job")
    job = get_job(job_id)
    if not job:
        return jsonify({"messages": [], "error": "job_not_found"}), 404
    return jsonify({"messages": job.result_messages, "error": job.error})

@app.route("/cancel_job", methods=["POST"])
def cancel_job():
    job_id = request.args.get("job") or request.form.get("job")
    job = get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "job_not_found"}), 404
    job.cancel()
    job.push(sse_format("log", "⛔ Stopverzoek ontvangen — breek netjes af…"))
    return jsonify({"ok": True})

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5003))
    app.run(debug=True, use_reloader=False, port=PORT, threaded=True)
