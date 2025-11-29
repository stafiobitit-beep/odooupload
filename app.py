# -*- coding: utf-8 -*-
import os
import uuid
import time
import logging
import threading
import pandas as pd
import xmlrpc.client
import requests
import re
import base64
from io import BytesIO
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

from openpyxl import load_workbook
from flask import Flask, request, render_template, redirect, url_for, session, jsonify
from flask_session import Session
GLOBAL_FAST_MODE = env_flag("FAST_MODE", default=False)

# =========================
# HTTP / Images tunings
# =========================
SESSION_HTTP = requests.Session()
MAX_IMAGE_WORKERS = int(os.environ.get("IMAGE_WORKERS", "12"))
MAX_IMG_PX = int(os.environ.get("MAX_IMG_PX", "1024"))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", "80"))
MAX_IMG_BYTES = int(os.environ.get("MAX_IMG_BYTES", str(6 * 1024 * 1024)))  # 6 MB

# =========================
# Background Job registry
# =========================
# We koppelen een job aan de huidige sessie via job_id.
JOBS = {}  # job_id -> {"processed":int, "total":int, "start":float, "done":bool, "messages":[...], "error":str|None}
JOBS_LOCK = threading.Lock()

def new_job():
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {"processed": 0, "total": 0, "start": time.time(), "done": False, "messages": [], "error": None}
    return job_id

def job_set_total(job_id, total):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["total"] = int(total)

def job_tick(job_id, delta=1):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["processed"] += int(delta)

def job_msg(job_id, msg):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["messages"].append(msg)

def job_fail(job_id, err):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["error"] = str(err)
            JOBS[job_id]["done"] = True

def job_done(job_id):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id]["done"] = True

def job_get(job_id):
    with JOBS_LOCK:
        return JOBS.get(job_id, None)

# =========================
# XML-RPC transport (requests)
# =========================
class RequestsTransport(xmlrpc.client.Transport):
    def request(self, host, handler, request_body, verbose=False):
        host = host.rstrip("/")
        handler = handler.lstrip("/")
        url = f"{'https://' if not host.startswith('http') else ''}{host}/{handler}"
        headers = {"User-Agent": self.user_agent, "Content-Type": "text/xml"}
        resp = requests.post(url, data=request_body, headers=headers, verify=True, allow_redirects=False, timeout=60)
        if resp.is_redirect or resp.status_code in (301, 302, 303, 307, 308):
            raise Exception(f"Unexpected redirect to {resp.headers.get('Location')}")
        resp.raise_for_status()
        return self.parse_response(resp)

    def parse_response(self, response):
        if "text/xml" not in (response.headers.get("Content-Type") or ""):
            raise Exception(f"Unexpected content type: {response.headers.get('Content-Type')}\n{response.text[:400]}")
        p, u = self.getparser()
        p.feed(response.content)
        return u.close()

# =========================
# Utils & helpers
# =========================
MAX_XMLRPC_INT = 2**31 - 1
TRANSLATABLE_FIELDS = {"name", "description_sale", "website_description"}

TRANSLATION_COL_REGEXES = [
    re.compile(r'^\s*(name|description_sale|website_description)\s*\(([a-zA-Z]{2}[_-][a-zA-Z]{2})\)\s*$'),
    re.compile(r'^\s*(name|description_sale|website_description)\s*\[([a-zA-Z]{2}[_-][a-zA-Z]{2})\]\s*$'),
]

CATEGORY_SENTINELS = {
    "", "-", "van categorie", "from category", "inherit", "category", "use category",
    "categorie", "categorie-instelling", "default", "none"
}

INVOICE_POLICY_ALIASES = {
    "order": {"order","ordered","ordered quantities","bestel","bestelde","bestelde hoeveelheden","commandé","commandées","quantités commandées"},
    "delivery": {"delivery","delivered","delivered quantities","geleverde","geleverde hoeveelheden","livraison","livrées","quantités livrées"},
}

def _canonical_invoice_policy(raw):
    if raw is None:
        return None
    s = str(raw).strip().lower().replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    if s in INVOICE_POLICY_ALIASES["order"]:
        return "order"
    if s in INVOICE_POLICY_ALIASES["delivery"]:
        return "delivery"
    if s.startswith(("gelev", "livr")):
        return "delivery"
    if s.startswith(("bestel", "command")):
        return "order"
    return None

def is_category_inherit(val) -> bool:
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return False
    return str(val).strip().lower() in CATEGORY_SENTINELS

def retry(func, *args, retries=6, backoff=1.7, **kwargs):
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            if getattr(e.response, "status_code", None) == 429:
                time.sleep(backoff ** i)
                continue
            raise
        except xmlrpc.client.Fault:
            raise
    raise RuntimeError("Max retries reached")

def normalize_lang_code(code: str) -> str:
    code = (code or "").replace("-", "_")
    parts = code.split("_")
    return f"{parts[0].lower()}_{parts[1].upper()}" if len(parts) == 2 else code

def company_ctx(company_id, lang=None):
    ctx = {"tracking_disable": True, "mail_notrack": True}
    if lang:
        ctx["lang"] = normalize_lang_code(lang)
    if company_id:
        ctx["force_company"] = int(company_id)
        ctx["allowed_company_ids"] = [int(company_id)]
    return ctx

def parse_decimal(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    if "," in s and "." not in s:
        s = s.replace(" ", "").replace(".", "")
        s = s.replace(",", ".")
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
    if d is None:
        return "0"
    s = format(d.normalize(), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"

def to_text_code(val):
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s[:-2] if s.endswith(".0") else s
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        try:
            dec = Decimal(str(val))
            return str(int(dec)) if dec == dec.to_integral() else format(dec.normalize(), "f")
        except InvalidOperation:
            return str(val)
    return str(val)

def _coerce_id(x):
    if isinstance(x, int):
        return x
    if isinstance(x, (list, tuple)):
        if not x:
            raise ValueError("Lege lijst kan geen id zijn")
        return _coerce_id(x[0])
    try:
        return int(Decimal(str(x)))
    except Exception:
        raise ValueError(f"Invalid id payload voor int(): {x!r}")

def ensure_ids_list(ids):
    if ids is None:
        return []
    if isinstance(ids, (list, tuple)):
        return [_coerce_id(i) for i in ids]
    return [_coerce_id(ids)]

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def norm_name_for_match(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip().replace("\u00A0", " ")
    import unicodedata
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[’'`´]", "'", s)
    s = re.sub(r"[^a-z0-9 ']", "", s, flags=re.IGNORECASE)
    return s.lower()

# =========================
# Per-run cache
# =========================
class RunCache:
    def __init__(self):
        self.fields_meta = {}
        self.m2o = {}
        self.m2m_split = {}
        self.categories = {}
        self.uom_by_norm = {}
        self.tax_by_name = {}
        self.tag_by_name = {}
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

CACHE = None

def model_has_field(models, db, uid, key, model, field_name):
    try:
        fget = retry(
            models.execute_kw, db, uid, key, model, "fields_get",
            [], {"attributes": ["type","relation"]}
        )
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

# =========================
# Companies / langs
# =========================
def get_companies(models, db, uid, key):
    try:
        recs = retry(
            models.execute_kw, db, uid, key, "res.company", "search_read",
            [[]], {"fields": ["id", "name"], "limit": 200}
        )
        recs.sort(key=lambda r: (r.get("name") or "").lower())
        return recs
    except Exception:
        return []

def get_active_languages(models, db, uid, key):
    try:
        recs = retry(
            models.execute_kw, db, uid, key, "res.lang", "search_read",
            [[("active", "=", True)]], {"fields": ["code", "name"], "limit": 200}
        )
        langs = []
        for r in recs:
            code = normalize_lang_code(r.get("code") or "")
            if code:
                langs.append((code, r.get("name") or code))
        return langs or [("nl_BE", "Dutch (Belgium)"), ("fr_BE", "French (Belgium)"), ("en_US", "English (US)")]
    except Exception:
        return [("nl_BE", "Dutch (Belgium)"), ("fr_BE", "French (Belgium)"), ("en_US", "English (US)")]

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

# =========================
# Accounts
# =========================
def _account_schema(models, db, uid, key):
    has_account_type = model_has_field(models, db, uid, key, "account.account", "account_type")
    has_user_type = model_has_field(models, db, uid, key, "account.account", "user_type_id")
    return has_account_type, has_user_type

def _find_account_type_id(models, db, uid, key, kind):
    try:
        types = retry(
            models.execute_kw, db, uid, key, "account.account.type", "search_read",
            [[("internal_group", "=", kind)]], {"fields": ["id"], "limit": 1}
        )
        if types:
            return types[0]["id"]
    except Exception:
        pass
    name_q = "Income" if kind == "income" else "Expenses"
    try:
        types = retry(
            models.execute_kw, db, uid, key, "account.account.type", "search",
            [[("name", "ilike", name_q)]], {"limit": 1}
        )
        if types:
            return types[0]
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
        CACHE.account_cache[k] = ids[0]
        return ids[0]

    s6 = _normalize_account_code(s)
    if s6 != s:
        dom = dom_company + [("code", "=", s6)]
        ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1})
        if ids:
            CACHE.account_cache[k] = ids[0]
            return ids[0]

    dom = dom_company + [("code", "ilike", f"{s}%")]
    ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1, "order": "code asc"})
    if ids:
        CACHE.account_cache[k] = ids[0]
        return ids[0]

    dom = dom_company + [("name", "ilike", s)]
    ids = retry(models.execute_kw, db, uid, key, "account.account", "search", [dom], {"limit": 1})
    if ids:
        CACHE.account_cache[k] = ids[0]
        return ids[0]

    has_account_type, has_user_type = _account_schema(models, db, uid, key)
    vals = {
        "code": s6,
        "name": f"{'Income' if kind=='income' else 'Expense'} {s}",
        "reconcile": False,
    }
    if company_id and model_has_field(models, db, uid, key, "account.account", "company_id"):
        vals["company_id"] = int(company_id)

    if has_account_type:
        vals["account_type"] = "income" if kind == "income" else "expense"
    elif has_user_type:
        at_id = _find_account_type_id(models, db, uid, key, "income" if kind == "income" else "expense")
        if at_id:
            vals["user_type_id"] = int(at_id)

    try:
        new_id = retry(models.execute_kw, db, uid, key, "account.account", "create", [[vals]])
        CACHE.account_cache[k] = new_id
        return new_id
    except xmlrpc.client.Fault as e:
        logging.warning(f"Kon account niet creëren voor '{s}' ({kind}): {e}")
        return None

def assert_account_company(models, db, uid, key, account_id, company_id):
    if not company_id:
        return
    try:
        if not model_has_field(models, db, uid, key, "account.account", "company_id"):
            return
        rec = retry(
            models.execute_kw, db, uid, key, "account.account", "read",
            [[int(account_id)], ["company_id"]]
        )
        comp = (rec and rec[0].get("company_id") and rec[0]["company_id"][0]) or False
        if comp not in (False, int(company_id)):
            raise ValueError(f"Account {account_id} behoort tot andere company (heeft company_id={comp}).")
    except Exception:
        return

def apply_account_property(models, db, uid, key, product_id, field_name, account_id_or_none, company_id):
    if account_id_or_none is None:
        vals = {field_name: False}
    else:
        vals = {field_name: int(_coerce_id(account_id_or_none))}
    retry(
        models.execute_kw, db, uid, key,
        "product.template", "write",
        [[int(_coerce_id(product_id))], vals],
        {"context": company_ctx(company_id)}
    )

# =========================
# UoM
# =========================
class UoMResolver:
    def load(self, models, db, uid, key):
        global CACHE
        try:
            uoms = retry(
                models.execute_kw, db, uid, key, "uom.uom", "search_read",
                [[]], {"fields": ["id", "name", "display_name"], "limit": 10000}
            )
        except Exception as e:
            logging.warning(f"UoM load failed: {e}")
            uoms = []
        for r in uoms:
            for candidate in (r.get("name"), r.get("display_name")):
                if candidate:
                    CACHE.uom_by_norm.setdefault(_norm(candidate), r["id"])

    def get(self, models, db, uid, key, raw_value, company_ctx_dict=None):
        global CACHE
        if not raw_value:
            return None
        name = str(raw_value).strip()
        n = _norm(name)
        if n in CACHE.uom_by_norm:
            return CACHE.uom_by_norm[n]
        try:
            hit = retry(
                models.execute_kw, db, uid, key, "uom.uom", "search",
                [[("name", "ilike", name)]], {"limit": 1, "context": (company_ctx_dict or {})}
            )
            if hit:
                CACHE.uom_by_norm[n] = hit[0]
                return hit[0]
        except Exception:
            pass
        return None

UOM = UoMResolver()

# =========================
# Dynamic field resolvers
# =========================
def _coerce_bool(v):
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "ja", "y")

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

def _relation_char_fields(meta_fields):
    codeish, nameish, others = [], [], []
    for fname, meta in (meta_fields or {}).items():
        if (meta or {}).get("type") != "char":
            continue
        low = fname.lower()
        if any(tok in low for tok in ("code", "ref", "sku", "nummer", "nr", "cod", "key", "korting", "discount", "group", "groep")):
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
        res = retry(
            models.execute_kw, db, uid, key, relation, "name_search",
            [str(value)],
            {"operator": "ilike", "limit": int(limit), "context": company_ctx(company_id)}
        )
        if res:
            return [int(_coerce_id(r[0])) for r in res]
    except Exception:
        pass
    return []

def _search_on_fields(models, db, uid, key, relation, field_name, value, company_id=None, limit=1, exact=True):
    op = "=" if exact else "ilike"
    dom = [(field_name, op, value if exact else str(value))]
    try:
        ids = retry(
            models.execute_kw, db, uid, key, relation, "search",
            [dom],
            {"limit": int(limit), "context": company_ctx(company_id)}
        )
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
    codeish, nameish, others = _relation_char_fields(rel_fields)
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

# Routes helpers
ROUTE_ALIASES = {
    "kopen": ["Kopen", "Buy"],
    "buy": ["Buy", "Kopen"],
    "mto": ["MTO", "Make To Order", "Replenish on Order (MTO)", "Aanvullen op bestelling (MTO)", "Aanvullen per order (MTO)"],
    "aanvullen op bestelling": ["Aanvullen op bestelling (MTO)", "Replenish on Order (MTO)", "Make To Order", "MTO"],
    "aanvullen per order": ["Aanvullen per order (MTO)", "Aanvullen op bestelling (MTO)", "Replenish on Order (MTO)", "Make To Order", "MTO"],
}

def _stock_route_candidates(label: str):
    s = str(label or "").strip()
    low = s.lower()
    cands = [s]
    for key, arr in ROUTE_ALIASES.items():
        if key in low or s in arr:
            cands.extend(arr)
    if "mto" in low:
        cands.extend(ROUTE_ALIASES["mto"])
    if "kopen" in low or "buy" in low:
        cands.extend(ROUTE_ALIASES["kopen"])
    return list(dict.fromkeys([c for c in cands if c]))

def _prefetch_routes(models, db, uid, key, company_id=None):
    global CACHE
    if CACHE.routes_loaded:
        return
    try:
        recs = retry(
            models.execute_kw, db, uid, key, "stock.route", "search_read",
            [[]],
            {"fields":["id","name"], "limit": 10000, "context": company_ctx(company_id)}
        )
        for r in recs or []:
            nm = r.get("name") or ""
            if nm:
                CACHE.routes[_norm(nm)] = int(_coerce_id(r["id"]))
    except Exception:
        pass
    CACHE.routes_loaded = True

def _resolve_stock_route_ids(models, db, uid, key, raw, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "":
        return []
    _prefetch_routes(models, db, uid, key, company_id=company_id)
    items = re.split(r"[,\n;\|]", str(raw))
    out = []
    for p in (t.strip() for t in items):
        if not p:
            continue
        n = _norm(p)
        rid = CACHE.routes.get(n)
        if rid:
            if rid not in out:
                out.append(rid)
            continue
        found = None
        for cand in _stock_route_candidates(p):
            rid = CACHE.routes.get(_norm(cand))
            if rid:
                found = rid
                break
        if found and found not in out:
            out.append(found)
    return out

def _resolve_many2many(models, db, uid, key, relation, raw, company_id=None):
    global CACHE
    if raw is None or str(raw).strip() == "":
        return []
    if relation == "stock.route":
        return _resolve_stock_route_ids(models, db, uid, key, raw, company_id=company_id)

    items = re.split(r"[,\n;\|]", str(raw))
    ids = []
    for p in (t.strip() for t in items):
        if not p:
            continue
        kn = (relation, _norm(p), int(company_id or 0))
        if kn in CACHE.m2m_split:
            mid = CACHE.m2m_split[kn]
        else:
            mid = _resolve_many2one(models, db, uid, key, relation, p, company_id)
            if mid:
                CACHE.m2m_split[kn] = int(_coerce_id(mid))
        if mid:
            mid = int(_coerce_id(mid))
            if mid not in ids:
                ids.append(mid)
    return ids

def resolve_dynamic_field(models, db, uid, key, field_name, raw, company_id=None):
    meta = _fields_get_cached(models, db, uid, key, "product.template").get(field_name) or {}
    ftype = meta.get("type")
    if not ftype:
        return (False, raw)

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
    if ftype == "many2one":
        rel = meta.get("relation")
        mid = _resolve_many2one(models, db, uid, key, rel, raw, company_id)
        return (False, mid)
    if ftype in ("many2many", "one2many"):
        rel = meta.get("relation")
        mids = _resolve_many2many(models, db, uid, key, rel, raw, company_id)
        return (True, mids)
    return (False, raw)

# =========================
# Warehouses / Locations / Put-away
# =========================
def _find_wh_by_code(models, db, uid, key, code, company_id=None):
    global CACHE
    k = (_norm(code), int(company_id or 0))
    if k in CACHE.wh_code_id:
        return CACHE.wh_code_id[k]
    dom = [("code", "=", str(code).strip())]
    if company_id and model_has_field(models, db, uid, key, "stock.warehouse", "company_id"):
        dom.append(("company_id", "in", [int(company_id), False]))
    ids = retry(models.execute_kw, db, uid, key, "stock.warehouse", "search", [dom], {"limit": 1})
    wh = ids and ids[0] or None
    CACHE.wh_code_id[k] = wh
    return wh

def _read_wh_roots(models, db, uid, key, wh_id):
    global CACHE
    if wh_id in CACHE.wh_roots:
        return CACHE.wh_roots[wh_id]
    data = retry(
        models.execute_kw, db, uid, key, "stock.warehouse", "read",
        [[int(wh_id)], ["lot_stock_id", "wh_input_stock_loc_id", "wh_output_stock_loc_id", "view_location_id","code"]]
    )[0]
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
    if wh:
        return wh
    dom = []
    if company_id and model_has_field(models, db, uid, key, "stock.warehouse", "company_id"):
        dom = [("company_id", "in", [int(company_id), False])]
    ids = retry(models.execute_kw, db, uid, key, "stock.warehouse", "search", [dom], {"limit": 1})
    return ids and ids[0] or None

def get_or_create_location_by_path(models, db, uid, key, path, company_id=None, create_missing=True):
    global CACHE
    if not path:
        return None
    raw = str(path).strip().replace("\\", "/")
    parts = [p for p in (seg.strip() for seg in raw.split("/")) if p]
    if len(parts) == 0:
        return None

    kn = (_norm(raw), int(company_id or 0))
    if kn in CACHE.loc_path:
        return CACHE.loc_path[kn]

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
            current_parent = stock_root or current_parent
            i += 1
        elif alias == "input":
            current_parent = roots.get("input") or current_parent
            i += 1
        elif alias == "output":
            current_parent = roots.get("output") or current_parent
            i += 1
        else:
            current_parent = stock_root or current_parent

    for j in range(i, len(parts)):
        seg = parts[j]
        dom = [("name", "=", seg), ("location_id", "=", int(_coerce_id(current_parent)))]
        if company_id and model_has_field(models, db, uid, key, "stock.location", "company_id"):
            dom.append(("company_id", "in", [int(company_id), False]))
        ids = retry(models.execute_kw, db, uid, key, "stock.location", "search", [dom], {"limit": 1})
        if ids:
            current_parent = ids[0]
            continue

        dom = [("name", "ilike", seg), ("location_id", "=", int(_coerce_id(current_parent)))]
        if company_id and model_has_field(models, db, uid, key, "stock.location", "company_id"):
            dom.append(("company_id", "in", [int(company_id), False]))
        ids = retry(models.execute_kw, db, uid, key, "stock.location", "search", [dom], {"limit": 1})
        if ids:
            current_parent = ids[0]
            continue

        if not create_missing:
            return None

        is_leaf = (j == len(parts) - 1)
        vals = {
            "name": seg,
            "location_id": int(_coerce_id(current_parent)),
            "usage": "internal" if is_leaf else "view",
            "active": True,
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
        if name in cand_apply:
            apply_field = name
            break
    if not apply_field and cand_apply:
        apply_field = cand_apply[0]

    dest_field = None
    for name in ["putaway_location_id", "location_dest_id", "fixed_location_id"]:
        if name in meta and meta[name].get("relation")=="stock.location":
            dest_field = name
            break
    if not dest_field:
        for f in cand_apply:
            if f != apply_field:
                dest_field = f
                break

    return prod_field, apply_field, dest_field

def _get_variant_id(models, db, uid, key, product_tmpl_id, ctx=None):
    try:
        rec = retry(
            models.execute_kw, db, uid, key,
            "product.template", "read",
            [ensure_ids_list(product_tmpl_id), ["product_variant_id"]],
            ctx or {}
        )
        if rec and rec[0].get("product_variant_id"):
            return _coerce_id(rec[0]["product_variant_id"])
    except Exception:
        pass
    try:
        ids = retry(
            models.execute_kw, db, uid, key,
            "product.product", "search",
            [[("product_tmpl_id", "=", int(_coerce_id(product_tmpl_id)))]],
            {"limit": 1}
        )
        if ids:
            return _coerce_id(ids[0])
    except Exception:
        pass
    return None

def create_or_update_putaway_rule(models, db, uid, key, product_tmpl_id, company_id, wh_code, leaf_code):
    global CACHE
    wh_id = _find_wh_by_code(models, db, uid, key, wh_code, company_id=company_id)
    if not wh_id:
        wh_id = _get_default_warehouse_id(models, db, uid, key, company_id=company_id)
    if not wh_id:
        raise ValueError("Geen magazijn gevonden voor put-away aanmaak.")

    roots = _read_wh_roots(models, db, uid, key, wh_id)
    stock_root = roots.get("stock")
    if not stock_root:
        raise ValueError("Stock-root van magazijn niet gevonden.")

    path = f"{roots.get('code')}/Stock/{str(leaf_code).strip()}"
    dest_loc_id = get_or_create_location_by_path(models, db, uid, key, path, company_id=company_id, create_missing=True)

    prod_field, apply_field, dest_field = _detect_putaway_fields(models, db, uid, key)
    if not (apply_field and dest_field):
        raise ValueError("Kon veldnamen van stock.putaway.rule niet detecteren.")

    ctx = {"context": company_ctx(company_id)}
    payload_filter = [(apply_field, "=", int(_coerce_id(stock_root)))]
    payload_vals = {apply_field: int(_coerce_id(stock_root)), dest_field: int(_coerce_id(dest_loc_id))}

    product_key = None
    if prod_field == "product_id":
        variant_id = _get_variant_id(models, db, uid, key, product_tmpl_id, ctx)
        if not variant_id:
            raise ValueError("Geen productvariant gevonden voor put-away rule.")
        payload_filter.append((prod_field, "=", int(_coerce_id(variant_id))))
        payload_vals[prod_field] = int(_coerce_id(variant_id))
        product_key = ("pp", int(_coerce_id(variant_id)))
    elif prod_field == "product_tmpl_id":
        payload_filter.append((prod_field, "=", int(_coerce_id(product_tmpl_id))))
        payload_vals[prod_field] = int(_coerce_id(product_tmpl_id))
        product_key = ("pt", int(_coerce_id(product_tmpl_id)))
    else:
        raise ValueError("Put-away model heeft geen product-veld (product_id/product_tmpl_id).")

    cache_key = (product_key, int(_coerce_id(stock_root)))
    if cache_key in CACHE.putaway_seen:
        rid = CACHE.putaway_seen[cache_key]
        try:
            retry(
                models.execute_kw, db, uid, key,
                "stock.putaway.rule", "write",
                [ensure_ids_list(rid), {dest_field: int(_coerce_id(dest_loc_id))}],
                {"context": company_ctx(company_id)}
            )
        except Exception:
            pass
        return rid, dest_loc_id

    rid = retry(
        models.execute_kw, db, uid, key,
        "stock.putaway.rule", "search",
        [payload_filter],
        {"limit": 1, "context": company_ctx(company_id)}
    )
    if rid:
        retry(
            models.execute_kw, db, uid, key,
            "stock.putaway.rule", "write",
            [ensure_ids_list(rid[0]), payload_vals],
            {"context": company_ctx(company_id)}
        )
        CACHE.putaway_seen[cache_key] = rid[0]
        return rid[0], dest_loc_id
    else:
        new_id = retry(
            models.execute_kw, db, uid, key,
            "stock.putaway.rule", "create",
            [[payload_vals]],
            {"context": company_ctx(company_id)}
        )
        CACHE.putaway_seen[cache_key] = new_id
        return new_id, dest_loc_id

# =========================
# Images
# =========================
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
    global CACHE
    key = _norm(url)
    if key in CACHE.image_by_url:
        return CACHE.image_by_url[key]

    max_px = max_px or MAX_IMG_PX

    try:
        h = SESSION_HTTP.head(url, timeout=(3, 8), allow_redirects=True)
        if h.ok:
            cl = h.headers.get("content-length")
            if cl and cl.isdigit() and int(cl) > MAX_IMG_BYTES:
                raise Exception(f"image too large: {cl} bytes")
    except Exception:
        pass

    resp = SESSION_HTTP.get(url, timeout=(3, 20))
    resp.raise_for_status()
    content = resp.content
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
    b64, mimetype, fname = _download_and_prepare_image(url)
    field = _best_image_field_for_product(models, db, uid, key)
    retry(
        models.execute_kw, db, uid, key,
        "product.template", "write",
        [ensure_ids_list(product_id), {field: b64}],
        {"context": company_ctx(company_id)}
    )

def _ensure_gallery_image(models, db, uid, key, product_tmpl_id, company_id, url):
    b64, mimetype, fname = _download_and_prepare_image(url)
    # product.image?
    try:
        retry(models.execute_kw, db, uid, key, "product.image", "fields_get", [], {"attributes":["type"]})
        img_field = _best_image_field_for_gallery(models, db, uid, key)
        vals = {"name": fname, "product_tmpl_id": int(_coerce_id(product_tmpl_id)), img_field: b64, "active": True}
        retry(models.execute_kw, db, uid, key, "product.image", "create", [[vals]], {"context": company_ctx(company_id)})
        return
    except Exception:
        pass
    # fallback: ir.attachment
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

# =========================
# UI fields (no duplicates) – include invoice_policy & route_ids
# =========================
FIELD_GROUPS = {
    "Algemeen": [
        ("name", "Naam (standaard)"),
        ("default_code", "Interne Referentie"),
        ("barcode", "Barcode"),
        ("detailed_type", "Product Type (Storable/Consumable/Service)"),
        ("categ_id", "Categorie (Product Category)"),
        ("uom_id", "Verkoop UoM (Eenheid)"),
        ("uom_po_id", "Aankoop UoM (Eenheid)"),
        ("weight", "Gewicht"),
        ("product_tag_ids", "Tags (komma gescheiden)"),
    ],
    "Verkoop": [
        ("list_price", "Verkoopprijs"),
        ("taxes_id", "BTW/Taksen (namen, komma gescheiden)"),
        ("sale_ok", "Verkoopbaar (True/False)"),
        ("public_categ_ids", "Website Categorieën (pad of komma gescheiden)"),
        ("is_published", "Gepubliceerd op Website (True/False)"),
        ("available_in_pos", "Beschikbaar in POS (True/False)"),
        ("invoice_policy", "Facturatiebeleid (bestelde/geleverde)"),
    ],
    "Logistiek / Routes": [
        ("tracking", "Tracering (none/lot/serial)"),
        ("responsible_id", "Verantwoordelijke (many2one)"),
        ("route_ids", "Routes (stock.route, m2m)"),
    ],
    "Aankoop / Leverancier": [
        ("purchase_ok", "Aankoopbaar (True/False)"),
        ("standard_price", "Kostprijs (Cost)"),
        ("supplier", "Leverancier Naam (virtueel)"),
        ("supplier_product_code", "Leverancier Productcode (virtueel)"),
        ("aankoopprijs", "Leveranciersprijs / Inkoopprijs (virtueel)"),
        ("min_order_qty", "Minimum Bestelhoeveelheid (virtueel)"),
    ],
    "Content (standaardtaal)": [
        ("description_sale", "Verkoopomschrijving (standaard)"),
        ("website_description", "Website Omschrijving (standaard)"),
        ("website_meta_title", "SEO Titel"),
        ("website_meta_description", "SEO Omschrijving"),
    ],
    "Voorraad & Locaties": [
        ("stock_quantity", "Voorraadhoeveelheid (virtueel)"),
        ("inventory_location_path", "Locatiepad voor voorraad (bv. WH/stock/123)"),
        ("inventory_putaway_code", "Put-away code (bv. 1F3D5)"),
    ],
    "Media": [
        ("image_url", "Afbeelding (URL) — hoofdafbeelding"),
        ("image_urls", "Extra afbeeldingen (URL’s, komma/; gescheiden)"),
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
    """
    Groepen:
    1) Vertalingen
    2) Curated FIELD_GROUPS (bovenaan)
    3) Alle overige velden automatisch, zonder dubbels/technische velden
    """
    try:
        all_fields = retry(
            models.execute_kw, db, uid, key,
            "product.template", "fields_get", [],
            {"attributes": ["string", "type", "relation"]}
        )
    except Exception:
        all_fields = {}

    grouped, seen_keys = [], set()

    # 1) Vertalingen
    default_lang = get_default_lang(models, db, uid, key)
    try:
        langs = get_active_languages(models, db, uid, key)
    except Exception:
        langs = [("nl_BE","Dutch (Belgium)"), ("fr_BE","French (Belgium)"), ("en_US","English (US)")]

    trans_group = []
    label_map = {"name": "Naam", "description_sale": "Verkoopomschrijving", "website_description": "Website Omschrijving"}
    for base in TRANSLATABLE_FIELDS:
        for code, _nm in langs:
            if normalize_lang_code(code) == normalize_lang_code(default_lang):
                continue
            key_ = f"{base}[{code}]"
            if key_ in seen_keys:
                continue
            label = f"{label_map.get(base, base)} ({code})"
            trans_group.append({"key": key_, "label": label})
            seen_keys.add(key_)
    if trans_group:
        grouped.append({"group": "Vertalingen", "fields": trans_group})

    # 2) Curated groepen
    VIRTUAL_KEYS = {
        "supplier", "supplier_product_code", "aankoopprijs", "min_order_qty",
        "stock_quantity", "RECUPEL", "BEBAT",
        "inventory_location_path", "inventory_putaway_code",
        "image_url", "image_urls",
    }
    for group_name, items in FIELD_GROUPS.items():
        present = []
        for (fname, fallback_label) in items:
            if fname in seen_keys:
                continue
            if fname in VIRTUAL_KEYS:
                present.append({"key": fname, "label": fallback_label})
                seen_keys.add(fname)
            elif fname in all_fields:
                label = (all_fields[fname].get("string") or fallback_label or fname)
                if fname in TRANSLATABLE_FIELDS and "(standaard)" not in label:
                    label = f"{label} (standaard)"
                present.append({"key": fname, "label": label})
                seen_keys.add(fname)
        if present:
            grouped.append({"group": group_name, "fields": present})

    # 3) Alle overige velden (zonder technische)
    EXCLUDE_TECH = {
        "id", "create_uid", "create_date", "write_uid", "write_date",
        "message_follower_ids", "message_partner_ids", "message_ids",
        "activity_ids", "activity_type_id", "activity_state",
        "activity_user_id", "activity_date_deadline", "activity_summary",
        "activity_exception_decoration", "activity_exception_icon",
        "message_is_follower", "message_has_error", "message_has_error_counter",
        "message_needaction", "message_needaction_counter", "message_unread",
        "message_unread_counter", "website_message_ids",
        "company_currency_id", "display_name",
        "product_variant_id", "product_variant_ids", "attribute_line_ids",
        "website_url", "website_id", "website_meta_keywords",
    }

    dynamic = []
    for fname, meta in (all_fields or {}).items():
        if fname in seen_keys:
            continue
        if fname in EXCLUDE_TECH:
            continue
        dynamic.append({"key": fname, "label": (meta.get("string") or fname)})

    if dynamic:
        dynamic.sort(key=lambda x: (x["label"] or "").lower())
        grouped.append({"group": "Alle velden", "fields": dynamic})

    return grouped

# =========================
# Routes
# =========================
@app.route("/")
def home():
    if "uid" in session:
        return redirect(url_for("upload_excel"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        url = request.form["url"].rstrip("/")
        db = request.form["db"]
        email = request.form["email"]
        api_key = request.form["api_key"]
        fast = request.form.get("fast") or request.args.get("fast") or ""
        session["fast_mode"] = str(fast).strip().lower() in ("1","true","yes","y","on")
        try:
            transport = RequestsTransport()
            common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common", transport=transport)
            uid = common.authenticate(db, email, api_key, {})
            if not uid:
                return render_template("login.html", message="Invalid credentials")
            session.update({"url": url, "db": db, "email": email, "api_key": api_key, "uid": uid})
            return redirect(url_for("upload_excel"))
        except Exception as e:
            return render_template("login.html", message=f"Error: {e}")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/progress")
def progress():
    job_id = session.get("job_id")
    j = job_get(job_id) if job_id else None
    if not j:
        # terugval op oude sessie-counters (voor achterwaartse compatibiliteit)
        processed = session.get("processed_rows", 0)
        total = session.get("total_rows", 0)
        start = session.get("start_time", time.time())
        eta = (time.time() - start) / processed * max(total - processed, 0) if processed > 0 else 0
        return jsonify({"processed": processed, "total": total, "time_remaining": eta, "done": processed >= total and total > 0, "error": None})
    # bereken ETA
    processed = j["processed"]
    total = j["total"]
    elapsed = max(time.time() - j["start"], 0.001)
    speed = processed / elapsed if elapsed > 0 else 0
    remaining = (total - processed) / speed if speed > 0 and total > 0 else 0
    return jsonify({
        "processed": processed,
        "total": total,
        "time_remaining": max(0, remaining),
        "done": bool(j["done"]),
        "done": bool(j["done"]),
        "error": j["error"],
        "messages": j["messages"]
    })

# =========================
# Excel Upload flow
# =========================
@app.route("/upload_excel", methods=["GET", "POST"])
def upload_excel():
    if "uid" not in session:
        return redirect(url_for("login"))
    if request.method == "GET":
        return render_template("excel_upload.html")
    if "excel_file" not in request.files:
        return render_template("excel_upload.html", message="Geen bestand geselecteerd.")
    f = request.files["excel_file"]
    if not f.filename.endswith(".xlsx"):
        return render_template("excel_upload.html", message="Upload een .xlsx bestand.")

    safe = secure_filename(f.filename) or "upload.xlsx"
    unique = f"{uuid.uuid4().hex}_{safe}"
    file_path = os.path.join(UPLOADS, unique)
    f.save(file_path)
    session["last_upload_path"] = file_path

    wb = load_workbook(file_path, data_only=True)
    sheets = wb.sheetnames
    return render_template("excel_upload.html", sheets=sheets, file_path=file_path)

@app.route("/select_sheet_excel", methods=["POST"])
def select_sheet_excel():
    if "uid" not in session:
        return redirect(url_for("login"))
    file_path = request.form.get("file_path") or session.get("last_upload_path")
    if not file_path or not os.path.exists(file_path):
        return render_template("excel_upload.html", message="Het geüploade Excel-bestand kon niet gevonden worden. Upload het bestand opnieuw alstublieft.")
    sheet = request.form["sheet"]

    df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
    columns = df.columns.tolist()
    example_row = df.iloc[0].to_dict() if not df.empty else {}

    try:
        transport = RequestsTransport()
        models = xmlrpc.client.ServerProxy(f'{session["url"]}/xmlrpc/2/object', transport=transport)
        grouped_fields = _build_clean_grouped_fields(models, session["db"], session["uid"], session["api_key"])
        langs = get_active_languages(models, session["db"], session["uid"], session["api_key"])
        default_lang = get_default_lang(models, session["db"], session["uid"], session["api_key"])
        companies = get_companies(models, session["db"], session["uid"], session["api_key"])
        user_company_id = get_user_company_id(models, session["db"], session["uid"], session["api_key"])
    except Exception as e:
        return render_template("excel_upload.html",
                               message=f"Kan velden/talen/bedrijven niet laden: {e}",
                               sheets=load_workbook(file_path, data_only=True).sheetnames,
                               file_path=file_path,
                               sheet_name=sheet,
                               columns=columns,
                               grouped_fields=[],
                               example_row=example_row)

    # reset progress on new selection
    session["job_id"] = None
    session["processed_rows"] = 0
    session["total_rows"] = 0
    session["start_time"] = time.time()

    return render_template("excel_upload.html",
                           sheets=load_workbook(file_path, data_only=True).sheetnames,
                           file_path=file_path,
                           sheet_name=sheet,
                           columns=columns,
                           grouped_fields=grouped_fields,
                           example_row=example_row,
                           langs=langs,
                           default_lang=default_lang,
                           companies=companies,
                           selected_company_id=user_company_id,
                           current_fast=session.get("fast_mode", GLOBAL_FAST_MODE))

# =========================
# Import Runner (background)
# =========================
def _run_import(job_id, payload):
    """
    Draait in aparte thread zodat de UI kan blijven poll'en.
    """
    global CACHE
    try:
        CACHE = RunCache()
        CACHE.product_barcode_map = {}  # id -> barcode

        url = payload["url"]
        db = payload["db"]
        uid = payload["uid"]
        key = payload["key"]
        file_path = payload["file_path"]
        sheet_name = payload["sheet_name"]
        chosen_company_id = payload["company_id"]
        fast_mode = payload["fast_mode"]
        skip_images = payload["skip_images"]
        img_workers = payload["img_workers"]

        transport = RequestsTransport()
        models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object", transport=transport)
        default_lang = get_default_lang(models, db, uid, key)
        base_company_id = chosen_company_id or get_user_company_id(models, db, uid, key)
        base_lang = normalize_lang_code(payload.get("base_lang") or default_lang)

        base_write_ctx = {"context": company_ctx(base_company_id, lang=base_lang)}
        ctx_company = {"context": company_ctx(base_company_id)}
        ctx_company_lang = {"context": company_ctx(base_company_id, lang=base_lang)}

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        except Exception as e:
            job_fail(job_id, f"Kon Excel niet lezen: {e}")
            return

        total = len(df)
        job_set_total(job_id, total)

        # verwijder upload na inlezen
        try:
            os.remove(file_path)
        except Exception:
            pass

        # Prefetch bestaande producten (barcode + naam)
        existing_by_barcode = {}
        existing_by_name_norm = {}
        try:
            offset, limit = 0, (2000 if fast_mode else 1000)
            while True:
                chunk = retry(
                    models.execute_kw, db, uid, key,
                    "product.template", "search_read",
                    [[], ["name", "barcode", "default_code"]],
                    {"limit": limit, "offset": offset, **ctx_company}
                )
                if not chunk:
                    break
                for p in chunk:
                    if p.get("barcode"):
                        existing_by_barcode[str(p["barcode"]).strip()] = p["id"]
                    if p.get("name"):
                        nn = norm_name_for_match(p["name"])
                        if nn:
                            existing_by_name_norm.setdefault(nn, p["id"])
                    # Track barcode for ID to handle "same name, different barcode" check
                    if p.get("id"):
                         # Store barcode if present, else empty string
                         CACHE.product_barcode_map[p["id"]] = str(p.get("barcode") or "").strip()
                offset += limit
                if len(chunk) < limit:
                    break
        except Exception:
            pass

        # Warmups
        try:
            UOM.load(models, db, uid, key)
        except Exception:
            pass
        _prefetch_routes(models, db, uid, key, company_id=base_company_id)

        default_wh_id = _get_default_warehouse_id(models, db, uid, key, company_id=base_company_id)
        if default_wh_id:
            wh_rec = retry(models.execute_kw, db, uid, key, "stock.warehouse", "read", [[int(default_wh_id)], ["code"]])
            default_wh_code = (wh_rec and wh_rec[0].get("code")) or "WH"
        else:
            default_wh_code = "WH"

        def to_bool(v):
            return v.strip().lower() in ("1", "y", "yes", "true", "ja") if isinstance(v, str) else bool(v)

        image_jobs = []   # (kind, product_id, company_id, url)

        # Hoofdloop
        for idx, row in df.iterrows():
            try:
                base_vals = {}
                m2m_vals = {}
                supplier_name = supplier_code = None
                buy_price = None
                min_qty = None
                stock_qty = None
                desired_location_path = None
                desired_location_id = None
                putaway_code = None

                image_main_url = None
                image_extra_urls = []

                mapped_tax_ids = []
                mapped_percent_ids = []
                bebat_id = None
                recupel_id = None

                translations_by_lang = {}
                std_fields_explicit = set()

                for col in df.columns:
                    raw = row[col]

                    matched_auto = False
                    for rx in TRANSLATION_COL_REGEXES:
                        m = rx.match(str(col))
                        if m:
                            base_field = m.group(1)
                            lang_code = normalize_lang_code(m.group(2))
                            if base_field in TRANSLATABLE_FIELDS and not (pd.isna(raw) or raw == ""):
                                translations_by_lang.setdefault(lang_code, {})[base_field] = raw
                            matched_auto = True
                            break
                    if matched_auto:
                        continue

                    field = payload["mapping"].get(col)
                    if not field or pd.isna(raw) or raw == "":
                        continue

                    is_translation_mapping = False
                    for rx in TRANSLATION_COL_REGEXES:
                        mm = rx.match(str(field))
                        if mm:
                            base_field = mm.group(1)
                            lang_code = normalize_lang_code(mm.group(2))
                            if base_field in TRANSLATABLE_FIELDS:
                                translations_by_lang.setdefault(lang_code, {})[base_field] = raw
                                is_translation_mapping = True
                            break
                    if is_translation_mapping:
                        continue

                    # virtuele velden
                    if field == "supplier":
                        supplier_name = str(raw)
                        continue
                    if field == "supplier_product_code":
                        supplier_code = to_text_code(raw)
                        continue
                    if field == "aankoopprijs":
                        d = parse_decimal(raw)
                        buy_price = float(d) if d is not None else 0.0
                        continue
                    if field == "min_order_qty":
                        try:
                            min_qty = int(float(str(raw).replace(",", ".")))
                        except Exception:
                            min_qty = 0
                        continue
                    if field == "stock_quantity":
                        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                            stock_qty = None
                        else:
                            val_str = str(raw).strip()
                            if not val_str or val_str.lower() in ("nan", "none", "-"):
                                stock_qty = None
                            else:
                                try:
                                    stock_qty = float(val_str.replace(",", "."))
                                except Exception:
                                    stock_qty = None
                        continue
                    if field == "inventory_location_path":
                        desired_location_path = str(raw).strip()
                        continue
                    if field == "inventory_putaway_code":
                        putaway_code = str(raw).strip()
                        continue
                    if field == "image_url":
                        image_main_url = str(raw).strip()
                        continue
                    if field == "image_urls":
                        image_extra_urls = [u.strip() for u in re.split(r"[,\n;\|]", str(raw)) if u and u.strip()]
                        continue

                    # echte productvelden
                    if field in TRANSLATABLE_FIELDS:
                        base_vals[field] = str(raw).strip()
                        std_fields_explicit.add(field)
                    elif field == "detailed_type":
                        val = str(raw).strip()
                        # laat ongeforceerd door (sommige Odoo's gebruiken 'product/consu/service')
                        base_vals["detailed_type"] = val
                    elif field == "categ_id":
                        base_vals["categ_id"] = get_or_create_category(models, db, uid, key, raw, company_id=base_company_id)
                    elif field in ("public_categ_ids", "pos_categ_ids"):
                        mname = "product.public.category" if field == "public_categ_ids" else "pos.category"
                        ids = []
                        for piece in str(raw).split(","):
                            cid = get_or_create_category(models, db, uid, key, piece.strip(), company_id=base_company_id, model_name=mname)
                            if cid:
                                ids.append(cid)
                        if ids:
                            m2m_vals[field] = [(6, 0, [_coerce_id(i) for i in ids])]
                    elif field == "product_tag_ids":
                        tag_ids = []
                        for t in str(raw).split(","):
                            tname = t.strip()
                            if not tname:
                                continue
                            kn = _norm(tname)
                            if kn in CACHE.tag_by_name:
                                tag_ids.append(CACHE.tag_by_name[kn])
                            else:
                                tid = retry(
                                    models.execute_kw, db, uid, key,
                                    "product.tag", "search",
                                    [[("name", "=", tname)]],
                                    {"limit": 1}
                                )
                                if tid:
                                    CACHE.tag_by_name[kn] = tid[0]
                                    tag_ids.append(tid[0])
                                else:
                                    new_tid = retry(
                                        models.execute_kw, db, uid, key,
                                        "product.tag", "create",
                                        [[{"name": tname}]]
                                    )
                                    CACHE.tag_by_name[kn] = new_tid
                                    tag_ids.append(new_tid)
                        if tag_ids:
                            m2m_vals["product_tag_ids"] = [(6, 0, [_coerce_id(i) for i in tag_ids])]
                    elif field in ("uom_id", "uom_po_id"):
                        uom_id = UOM.get(models, db, uid, key, raw, company_ctx(base_company_id))
                        if uom_id:
                            base_vals[field] = uom_id
                        else:
                            job_msg(job_id, f"Row {idx+1}: UoM '{raw}' niet gevonden voor veld '{field}'.")
                    elif field in ("available_in_pos", "is_published", "sale_ok", "purchase_ok"):
                        base_vals[field] = to_bool(raw)
                    elif field == "taxes_id":
                        for tx in str(raw).split(","):
                            txn = tx.strip()
                            if not txn:
                                continue
                            k = (_norm(txn), int(base_company_id or 0))
                            if k in CACHE.tax_by_name:
                                mapped_tax_ids.append(_coerce_id(CACHE.tax_by_name[k][0]))
                            else:
                                tid = retry(
                                    models.execute_kw, db, uid, key, "account.tax", "search",
                                    [[("name", "=", txn)]],
                                    {"limit": 1, "context": company_ctx(base_company_id)}
                                )
                                if tid:
                                    mapped_tax_ids.append(_coerce_id(tid[0]))
                                    rec = retry(
                                        models.execute_kw, db, uid, key, "account.tax", "read",
                                        [ensure_ids_list(tid[0]), ["amount_type"]],
                                        {"context": company_ctx(base_company_id)}
                                    )
                                    amt_type = (rec and rec[0].get("amount_type")) or None
                                    CACHE.tax_by_name[k] = (tid[0], amt_type)
                    elif field == "RECUPEL":
                        d = parse_decimal(raw)
                        if d is not None and d > 0:
                            name = f"Recupel({format_decimal_for_name(d)})"
                            recupel_id = get_or_create_tax(models, db, uid, key, name, company_id=base_company_id, amount=float(d), amount_type="fixed")
                    elif field == "BEBAT":
                        d = parse_decimal(raw)
                        if d is not None and d > 0:
                            name = f"Bebat({format_decimal_for_name(d)})"
                            bebat_id = get_or_create_tax(models, db, uid, key, name, company_id=base_company_id, amount=float(d), amount_type="fixed")
                    elif field in ("property_account_income_id", "property_account_expense_id"):
                        if is_category_inherit(raw):
                            base_vals[field] = "__USE_CATEGORY__"
                        else:
                            kind = "income" if field == "property_account_income_id" else "expense"
                            acc_id = find_or_create_account(models, db, uid, key, raw, kind, company_id=base_company_id)
                            if acc_id:
                                try:
                                    assert_account_company(models, db, uid, key, acc_id, base_company_id)
                                except Exception as ee:
                                    job_msg(job_id, f"Row {idx+1}: {field} → rekening overslagen ({ee})")
                                else:
                                    base_vals[field] = _coerce_id(acc_id)
                            else:
                                job_msg(job_id, f"Row {idx+1}: {field} → rekening '{raw}' niet gevonden/kon niet worden aangemaakt.")
                    elif field == "invoice_policy":
                        can = _canonical_invoice_policy(raw)
                        if can:
                            base_vals["invoice_policy"] = can
                        else:
                            is_m2m, coerced = resolve_dynamic_field(models, db, uid, key, field, raw, base_company_id)
                            if coerced in ("order","delivery"):
                                base_vals["invoice_policy"] = coerced
                            else:
                                job_msg(job_id, f"Row {idx+1}: veld 'invoice_policy' — waarde '{raw}' ongeldig/niet gevonden, overgeslagen.")
                    elif field == "barcode":
                        base_vals["barcode"] = to_text_code(raw)
                    else:
                        is_m2m, coerced = resolve_dynamic_field(models, db, uid, key, field, raw, base_company_id)
                        if is_m2m:
                            if coerced:
                                m2m_vals[field] = [(6, 0, [int(_coerce_id(i)) for i in coerced])]
                            else:
                                job_msg(job_id, f"Row {idx+1}: veld '{field}' — geen items gevonden voor waarde '{raw}', overgeslagen.")
                        else:
                            if coerced is None:
                                job_msg(job_id, f"Row {idx+1}: veld '{field}' — waarde '{raw}' ongeldig/niet gevonden, overgeslagen.")
                            else:
                                base_vals[field] = coerced

                # vertalingen in base_lang niet dubbel
                if base_lang in translations_by_lang:
                    for k in list(translations_by_lang[base_lang].keys()):
                        if k in TRANSLATABLE_FIELDS and (k in std_fields_explicit or k in base_vals):
                            translations_by_lang[base_lang].pop(k, None)
                    if not translations_by_lang.get(base_lang):
                        translations_by_lang.pop(base_lang, None)

                # PRODUCT LOOKUP — barcode → leverancierscode → naam
                product_id = None
                bc = base_vals.get("barcode")
                nm = (base_vals.get("name") or "").strip() if base_vals.get("name") else None

                if bc and bc in existing_by_barcode:
                    product_id = existing_by_barcode[bc]
                elif bc:
                    res = retry(models.execute_kw, db, uid, key, "product.template", "search", [[("barcode", "=", bc)]], {"limit": 1})
                    if res:
                        product_id = res[0]
                        existing_by_barcode[bc] = product_id

                if not product_id and nm:
                    nm_clean = nm.strip()
                    nm_norm = norm_name_for_match(nm_clean)
                    if nm_norm in existing_by_name_norm:
                        product_id = existing_by_name_norm[nm_norm]
                    else:
                        res = retry(models.execute_kw, db, uid, key, "product.template", "search", [[("name", "=", nm_clean)]], {"limit": 1})
                        if not res and not fast_mode:
                            res = retry(models.execute_kw, db, uid, key, "product.template", "search", [[("name", "ilike", nm_clean)]], {"limit": 1})
                        if res:
                            product_id = res[0]
                            if nm_norm:
                                existing_by_name_norm.setdefault(nm_norm, product_id)
                        else:
                            job_msg(job_id, f"Row {idx+1}: geen bestaand product gevonden op naam '{nm_clean}'")

                # LOGIC CHANGE: If found by name, check if barcode conflicts
                if product_id and bc:
                    # We found a product by name (or it was found by barcode earlier, but if by barcode, bc matches)
                    # If we found it by name, we must ensure we don't overwrite a product with a DIFFERENT barcode.
                    # If the existing product has a barcode, and it is NOT the same as 'bc', then treat as new.
                    existing_bc = CACHE.product_barcode_map.get(product_id)
                    # If we found by barcode earlier, existing_bc == bc.
                    # If we found by name, existing_bc might differ.
                    if existing_bc and existing_bc != bc:
                        job_msg(job_id, f"Row {idx+1}: Name match '{nm}' found (ID {product_id}) but has different barcode '{existing_bc}' vs '{bc}'. Creating new product.")
                        product_id = None


                prod_ctx = None
                prod_ctx_lang = None
                prod_company_id = base_company_id

                if product_id:
                    ids_arg = ensure_ids_list(product_id)
                    info = retry(models.execute_kw, db, uid, key, "product.template", "read", [ids_arg, ["company_id"]])
                    if info and info[0].get("company_id"):
                        prod_company_id = info[0]["company_id"][0]
                    prod_ctx = {"context": company_ctx(prod_company_id)}
                    prod_ctx_lang = {"context": company_ctx(prod_company_id, lang=base_lang)}

                    if base_vals:
                        safe_write_vals = {k: v for k, v in base_vals.items() if v != "__USE_CATEGORY__"}
                        if safe_write_vals:
                            retry(models.execute_kw, db, uid, key, "product.template", "write", [ids_arg, safe_write_vals], prod_ctx_lang)
                        job_msg(job_id, f"Row {idx+1}: Updated product '{nm}' (ID {product_id})")
                else:
                    if not base_vals.get("name"):
                        job_msg(job_id, f"Row {idx+1}: Fout: 'Naam (standaard)' ontbreekt en product kon niet worden aangemaakt.")
                        job_tick(job_id)
                        continue
                    create_vals = {k: v for k, v in base_vals.items() if v != "__USE_CATEGORY__"}
                    product_id = retry(models.execute_kw, db, uid, key, "product.template", "create", [[create_vals]], base_write_ctx)
                    job_msg(job_id, f"Row {idx+1}: Created product '{create_vals.get('name')}' (ID {product_id})")
                    ids_arg = ensure_ids_list(product_id)
                    info = retry(models.execute_kw, db, uid, key, "product.template", "read", [ids_arg, ["company_id"]])
                    if info and info[0].get("company_id"):
                        prod_company_id = info[0]["company_id"][0]
                    prod_ctx = {"context": company_ctx(prod_company_id)}
                    prod_ctx_lang = {"context": company_ctx(prod_company_id, lang=base_lang)}
                    if create_vals.get("name"):
                        nn = norm_name_for_match(create_vals["name"])
                        if nn:
                            existing_by_name_norm.setdefault(nn, product_id)

                # m2m na create/write (incl. route_ids)
                if m2m_vals:
                    retry(models.execute_kw, db, uid, key, "product.template", "write",
                          [ensure_ids_list(product_id), m2m_vals], prod_ctx)

                # images verzamelen
                if not skip_images:
                    if image_main_url:
                        image_jobs.append(("main", int(_coerce_id(product_id)), int(_coerce_id(prod_company_id)), image_main_url))
                    for u in image_extra_urls:
                        image_jobs.append(("extra", int(_coerce_id(product_id)), int(_coerce_id(prod_company_id)), u))

                # boekhouding properties
                try:
                    if prod_company_id and ("property_account_income_id" in base_vals or "property_account_expense_id" in base_vals):
                        if "property_account_income_id" in base_vals:
                            if base_vals["property_account_income_id"] == "__USE_CATEGORY__":  # erven van categorie
                                apply_account_property(models, db, uid, key, product_id, "property_account_income_id", None, prod_company_id)
                            elif base_vals["property_account_income_id"]:
                                apply_account_property(models, db, uid, key, product_id, "property_account_income_id", base_vals["property_account_income_id"], prod_company_id)
                        if "property_account_expense_id" in base_vals:
                            if base_vals["property_account_expense_id"] == "__USE_CATEGORY__":
                                apply_account_property(models, db, uid, key, product_id, "property_account_expense_id", None, prod_company_id)
                            elif base_vals["property_account_expense_id"]:
                                apply_account_property(models, db, uid, key, product_id, "property_account_expense_id", base_vals["property_account_expense_id"], prod_company_id)
                except Exception as _e:
                    job_msg(job_id, f"Row {idx+1}: waarschuwing (boekhoud-rekeningen): {_e}")

                # vertaalbare standaardvelden
                std_payload = {}
                for k in TRANSLATABLE_FIELDS:
                    if k in base_vals and base_vals.get(k) not in (None, "") and base_vals[k] != "__USE_CATEGORY__":
                        std_payload[k] = base_vals[k]
                if std_payload:
                    retry(models.execute_kw, db, uid, key, "product.template", "write", [ensure_ids_list(product_id), std_payload], prod_ctx_lang)

                # overige vertalingen
                if (not fast_mode) and translations_by_lang:
                    ids_arg = ensure_ids_list(product_id)
                    for lang_code, vals in list(translations_by_lang.items()):
                        if lang_code == base_lang:
                            continue
                        payload = {}
                        for k in TRANSLATABLE_FIELDS:
                            v = vals.get(k)
                            if v not in (None, ""):
                                payload[k] = v
                        if payload:
                            retry(
                                models.execute_kw, db, uid, key,
                                "product.template", "write",
                                [ids_arg, payload],
                                {"context": company_ctx(prod_company_id, lang=lang_code)}
                            )

                # locatiepad
                if desired_location_path:
                    try:
                        desired_location_id = get_or_create_location_by_path(models, db, uid, key, desired_location_path, company_id=prod_company_id, create_missing=True)
                    except Exception as e:
                        job_msg(job_id, f"Row {idx+1}: locatiepad '{desired_location_path}' niet gezet ({e})")
                        desired_location_id = None

                # put-away
                if putaway_code:
                    try:
                        wh_code = default_wh_code
                        rule_id, dest_loc_id = create_or_update_putaway_rule(models, db, uid, key, product_id, prod_company_id, wh_code, putaway_code)
                        if not fast_mode:
                            job_msg(job_id, f"Row {idx+1}: put-away rule ingesteld naar {wh_code}/Stock/{putaway_code} (rule id {rule_id}).")
                    except Exception as e:
                        job_msg(job_id, f"Row {idx+1}: put-away rule niet aangemaakt ({e}).")

                # voorraad
                if stock_qty is not None:
                    try:
                        prod_stock_ctx = {"context": company_ctx(prod_company_id)}
                        if desired_location_id:
                            loc_id = int(_coerce_id(desired_location_id))
                        else:
                            loc = retry(models.execute_kw, db, uid, key, "stock.location", "search", [[("usage", "=", "internal")]], {"limit": 1, **prod_stock_ctx})
                            loc_id = _coerce_id(loc[0]) if loc else None
                            if not loc_id:
                                job_msg(job_id, f"Row {idx+1}: geen interne locatie gevonden, voorraad niet aangepast.")
                        if loc_id:
                            variant = _get_variant_id(models, db, uid, key, product_id, prod_stock_ctx)
                            if not variant:
                                job_msg(job_id, f"Row {idx+1}: geen product_variant_id gevonden, voorraad niet aangepast.")
                            else:
                                quant = retry(models.execute_kw, db, uid, key, "stock.quant", "search",
                                              [[("product_id", "=", variant), ("location_id", "=", loc_id)]],
                                              {"limit": 1, **prod_stock_ctx})
                                if quant:
                                    retry(models.execute_kw, db, uid, key, "stock.quant", "write",
                                          [ensure_ids_list(quant[0]), {"quantity": float(stock_qty)}],
                                          prod_stock_ctx)
                                else:
                                    retry(models.execute_kw, db, uid, key, "stock.quant", "create",
                                          [[{"product_id": variant, "location_id": loc_id, "quantity": float(stock_qty)}]],
                                          prod_stock_ctx)
                                if not fast_mode:
                                    job_msg(job_id, f"Row {idx+1}: voorraad gezet op {stock_qty} (variant={variant}, locatie={loc_id})")
                    except Exception as e:
                        job_msg(job_id, f"Row {idx+1}: voorraad niet gezet ({e})")

                # leveranciersinfo
                if supplier_name and product_id:
                    try:
                        partner_id = get_or_create_supplier(models, db, uid, key, supplier_name, company_id=prod_company_id)
                        if not partner_id:
                            raise ValueError("Geen geldige leverancier-id")
                        try:
                            buy_val = float(buy_price) if buy_price is not None else 0.0
                        except Exception:
                            buy_val = 0.0
                        mq = int(min_qty or 0)
                        pt_id = _coerce_id(product_id)
                        prt_id = _coerce_id(partner_id)
                        dom = [("product_tmpl_id", "=", pt_id), ("partner_id", "=", prt_id), ("product_code", "=", supplier_code or "")]
                        sid = retry(models.execute_kw, db, uid, key, "product.supplierinfo", "search", [dom], {"limit": 1, "context": company_ctx(prod_company_id)})
                        vals = {"product_tmpl_id": pt_id, "partner_id": prt_id, "product_code": supplier_code or "", "price": buy_val, "min_qty": mq,
                                "company_id": int(_coerce_id(prod_company_id)) if prod_company_id else False}
                        if sid:
                            retry(models.execute_kw, db, uid, key, "product.supplierinfo", "write", [ensure_ids_list(sid[0]), vals], {"context": company_ctx(prod_company_id)})
                        else:
                            retry(models.execute_kw, db, uid, key, "product.supplierinfo", "create", [[vals]], {"context": company_ctx(prod_company_id)})
                    except Exception as e:
                        job_msg(job_id, f"Row {idx+1}: leverancierinfo niet gezet ({e})")

                # taxes finaliseren
                ids_arg = ensure_ids_list(product_id)
                for t in list(mapped_tax_ids):
                    found = False
                    for nk, val in list(CACHE.tax_by_name.items()):
                        if val[0] == t:
                            found = True
                            break
                    if not found:
                        rec = retry(models.execute_kw, db, uid, key, "account.tax", "read", [ensure_ids_list(t), ["amount_type"]],
                                    {"context": company_ctx(prod_company_id)})
                        amt_type = (rec and rec[0].get("amount_type")) or None
                        CACHE.tax_by_name[(f"id_{t}", int(prod_company_id or 0))] = (t, amt_type)

                for nk, (tid, amt_type) in CACHE.tax_by_name.items():
                    if tid in mapped_tax_ids and amt_type == "percent":
                        mapped_percent_ids.append(_coerce_id(tid))

                final_fixed_ids = set()
                for nk, (tid, amt_type) in CACHE.tax_by_name.items():
                    if tid in mapped_tax_ids and amt_type != "percent":
                        final_fixed_ids.add(_coerce_id(tid))
                if bebat_id:
                    final_fixed_ids.add(_coerce_id(bebat_id))
                if recupel_id:
                    final_fixed_ids.add(_coerce_id(recupel_id))

                if mapped_percent_ids:
                    final_percent = set(_coerce_id(x) for x in mapped_percent_ids)
                else:
                    vat21 = get_or_create_percent_tax(models, db, uid, key, 21.0, company_id=prod_company_id, preferred_name="VAT 21%")
                    final_percent = {_coerce_id(vat21)} if isinstance(vat21, int) else set()

                final_all = sorted(set(final_fixed_ids).union(final_percent))
                retry(models.execute_kw, db, uid, key, "product.template", "write", [ids_arg, {"taxes_id": [(6, 0, final_all)]}], {"context": company_ctx(prod_company_id)})

            except xmlrpc.client.Fault as e:
                job_msg(job_id, f"Row {idx+1}: XML-RPC Fault: {e}")
            except Exception as e:
                job_msg(job_id, f"Row {idx+1}: Fout: {e}")
            finally:
                job_tick(job_id)

        # Afbeeldingen parallel verwerken
        if image_jobs:
            try:
                with ThreadPoolExecutor(max_workers=img_workers) as pool:
                    futures = [pool.submit(_process_one_image, models, db, uid, key, kind, pid, cid, url)
                               for (kind, pid, cid, url) in image_jobs]
                    for fut in as_completed(futures):
                        try:
                            fut.result()
                        except Exception as e:
                            job_msg(job_id, f"Afbeeldingstaak: {e}")
            except Exception as e:
                job_msg(job_id, f"Parallelliseren van afbeeldingen faalde: {e}")

        job_done(job_id)

    except Exception as e:
        job_fail(job_id, e)

# =========================
# Form POST start → spawn background, re-render mapping page
# =========================
@app.route("/process_excel", methods=["POST"])
def process_excel():
    if "uid" not in session:
        return redirect(url_for("login"))

    file_path = request.form.get("file_path") or session.get("last_upload_path")
    sheet_name = request.form.get("sheet_name")
    if not file_path or not os.path.exists(file_path):
        return jsonify({"ok": False, "error": "Geen of ongeldig bestand. Upload opnieuw alstublieft."})

    # bouw mapping dict uit form
    # (we moeten de UI (mapForm) opnieuw tonen na starten; daarom laden we df/velden opnieuw)
    try:
        df_preview = pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Kon Excel niet lezen: {e}"})

    columns = df_preview.columns.tolist()
    example_row = df_preview.iloc[0].to_dict() if not df_preview.empty else {}

    transport = RequestsTransport()
    models = xmlrpc.client.ServerProxy(f'{session["url"]}/xmlrpc/2/object', transport=transport)
    grouped_fields = _build_clean_grouped_fields(models, session["db"], session["uid"], session["api_key"])
    langs = get_active_languages(models, session["db"], session["uid"], session["api_key"])
    default_lang = get_default_lang(models, session["db"], session["uid"], session["api_key"])
    companies = get_companies(models, session["db"], session["uid"], session["api_key"])
    user_company_id = get_user_company_id(models, session["db"], session["uid"], session["api_key"])

    # bedrijf + fast mode + images
    try:
        chosen_company_id = int(request.form.get("company_id") or 0) or None
    except Exception:
        chosen_company_id = None

    fast_mode_ui = (request.form.get("fast_mode") in ("1","true","yes","on"))
    if fast_mode_ui is not None:
        session["fast_mode"] = fast_mode_ui
    fast_mode = bool(session.get("fast_mode", GLOBAL_FAST_MODE))

    skip_images = (request.form.get("skip_images") == "1")
    try:
        img_workers = int(request.form.get("img_workers") or MAX_IMAGE_WORKERS)
    except Exception:
        img_workers = MAX_IMAGE_WORKERS

    base_lang = normalize_lang_code(request.form.get("base_lang") or default_lang)

    # mapping
    mapping = {}
    for col in columns:
        key = f"mapping[{col}]"
        mapping[col] = request.form.get(key) or ""

    # start background job
    job_id = new_job()
    session["job_id"] = job_id

    payload = {
        "url": session["url"],
        "db": session["db"],
        "uid": session["uid"],
        "key": session["api_key"],
        "file_path": file_path,
        "sheet_name": sheet_name,
        "company_id": chosen_company_id,
        "fast_mode": fast_mode,
        "skip_images": skip_images,
        "img_workers": img_workers,
        "base_lang": base_lang,
        "mapping": mapping,
    }
    threading.Thread(target=_run_import, args=(job_id, payload), daemon=True).start()

    # Render dezelfde mapping UI terug zodat JS kan blijven poll'en
    # Return JSON for frontend to start polling
    return jsonify({
        "ok": True,
        "job_id": job_id,
        "message": "Import gestart"
    })

# =========================
# Domain-specific helpers used above
# =========================
def get_or_create_supplier(models, db, uid, key, supplier_name, company_id=None):
    global CACHE
    name = str(supplier_name or "").strip()
    k = (_norm(name), int(company_id or 0))
    if name and k in CACHE.partner_by_name:
        return CACHE.partner_by_name[k]

    dom = [("name", "=", name), ("supplier_rank", ">", 0)]
    ids = retry(
        models.execute_kw, db, uid, key, "res.partner", "search",
        [dom], {"limit": 1, "context": company_ctx(company_id)}
    )
    if ids:
        CACHE.partner_by_name[k] = ids[0]
        return ids[0]
    nid = retry(
        models.execute_kw, db, uid, key, "res.partner", "create",
        [[{"name": name, "supplier_rank": 1}]], {"context": company_ctx(company_id)}
    )
    CACHE.partner_by_name[k] = nid
    return nid

def get_or_create_tax(models, db, uid, key, name, company_id=None, amount=None, amount_type=None):
    global CACHE
    s = str(name or "").strip()
    k = (_norm(s), int(company_id or 0))
    if k in CACHE.tax_by_name:
        return CACHE.tax_by_name[k][0]

    ids = retry(
        models.execute_kw, db, uid, key, "account.tax", "search",
        [[("name", "=", s)]], {"limit": 1, "context": company_ctx(company_id)}
    )
    if ids:
        rec = retry(
            models.execute_kw, db, uid, key, "account.tax", "read",
            [ensure_ids_list(ids[0]), ["amount_type"]],
            {"context": company_ctx(company_id)}
        )
        amt_type = (rec and rec[0].get("amount_type")) or None
        CACHE.tax_by_name[k] = (ids[0], amt_type)
        return ids[0]
    if amount_type is None:
        return None
    grp = retry(
        models.execute_kw, db, uid, key, "account.tax.group", "search",
        [[("name", "=", "All")]], {"limit": 1, "context": company_ctx(company_id)}
    )
    grp_id = grp[0] if grp else retry(
        models.execute_kw, db, uid, key, "account.tax.group", "create",
        [[{"name": "All"}]], {"context": company_ctx(company_id)}
    )
    data = {
        "name": s,
        "amount": float(amount or 0.0),
        "amount_type": amount_type,
        "type_tax_use": "sale",
        "tax_group_id": grp_id,
        "invoice_repartition_line_ids": [(0, 0, {"repartition_type": "base", "factor_percent": 100}),
                                         (0, 0, {"repartition_type": "tax", "factor_percent": 100})],
        "refund_repartition_line_ids":  [(0, 0, {"repartition_type": "base", "factor_percent": 100}),
                                         (0, 0, {"repartition_type": "tax", "factor_percent": 100})],
        "company_id": int(company_id) if company_id else False,
    }
    tid = retry(
        models.execute_kw, db, uid, key, "account.tax", "create",
        [[data]], {"context": company_ctx(company_id)}
    )
    CACHE.tax_by_name[k] = (tid, amount_type)
    return tid

def get_or_create_percent_tax(models, db, uid, key, percent, company_id=None, preferred_name=None):
    global CACHE
    k = (float(percent), int(company_id or 0))
    if k in CACHE.tax_percent_by_amount:
        return CACHE.tax_percent_by_amount[k]
    if preferred_name:
        tid = retry(
            models.execute_kw, db, uid, key, "account.tax", "search",
            [[("name", "=", preferred_name), ("amount_type", "=", "percent")]],
            {"limit": 1, "context": company_ctx(company_id)}
        )
        if tid:
            CACHE.tax_percent_by_amount[k] = tid[0]
            return tid[0]
    tids = retry(
        models.execute_kw, db, uid, key, "account.tax", "search",
        [[("amount", "=", float(percent)), ("amount_type", "=", "percent")]],
        {"limit": 1, "context": company_ctx(company_id)}
    )
    if tids:
        CACHE.tax_percent_by_amount[k] = tids[0]
        return tids[0]
    name = preferred_name or (f"VAT {int(percent)}%" if float(percent).is_integer() else f"VAT {percent}%")
    t = get_or_create_tax(models, db, uid, key, name, company_id=company_id, amount=float(percent), amount_type="percent")
    CACHE.tax_percent_by_amount[k] = t
    return t

# =========================
# Main
# =========================
if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5008))
    # In productie: debug=False, threaded=True om polling + background te laten samenwerken
    app.run(debug=True, use_reloader=False, port=PORT, threaded=True)
else:
    # When running under Gunicorn, log startup info
    logging.info("=" * 60)
    logging.info("🚀 Application starting under Gunicorn")
    logging.info(f"Python version: {os.sys.version}")
    logging.info(f"Flask app name: {app.name}")
    logging.info(f"Environment variables:")
    logging.info(f"  - PORT: {os.environ.get('PORT', 'not set')}")
    logging.info(f"  - APP_SECRET: {'set' if os.environ.get('APP_SECRET') else 'NOT SET (using default)'}")
    logging.info(f"  - FAST_MODE: {os.environ.get('FAST_MODE', 'not set')}")
    logging.info(f"  - IMAGE_WORKERS: {os.environ.get('IMAGE_WORKERS', 'not set')}")
    logging.info(f"Working directory: {os.getcwd()}")
    logging.info(f"Upload directory exists: {os.path.exists(UPLOADS)}")
    logging.info(f"Session directory exists: {os.path.exists('flask_session')}")
    logging.info("=" * 60)

# Error handlers for better logging
@app.errorhandler(500)
def internal_error(error):
    logging.error(f"Internal Server Error: {error}", exc_info=True)
    return "Internal Server Error - Check logs for details", 500

@app.errorhandler(Exception)
def handle_exception(e):
    logging.error(f"Unhandled exception: {e}", exc_info=True)
    return f"An error occurred: {str(e)}", 500

