# -*- coding: utf-8 -*-
"""
NFS-e (ADN) - Captura incremental por NSU + salva XMLs "soltos" no Storage
e gera ZIP do mês anterior automaticamente (na virada do mês).

✅ Salva XML individual a cada NSU (somente se for do mês-alvo = mês anterior)
✅ Dedup por hash do XML (não duplica no Storage)
✅ NSU salvo por CNPJ na tabela nsu_nfs (ponteiro incremental)
✅ Trata respostas que DEVEM pular e NÃO avançar NSU:
   - 404 JSON com StatusProcessamento=NENHUM_DOCUMENTO_LOCALIZADO
   - 400 JSON com StatusProcessamento=REJEICAO/REJEIÇÃO (ex.: E2214)
✅ Gera/atualiza ZIP do mês anterior no início do mês (dia 1..N)
   - monta ZIP a partir dos XMLs soltos no Storage
   - faz upload com upsert (sobrescreve o ZIP do mês)
"""

import os
import re
import time
import json
import base64
import gzip
import socket
import zipfile
import tempfile
import hashlib
import requests

from datetime import date, timedelta, datetime
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml import etree  # parse XML

# =========================================================
# === SUPABASE (CREDENCIAIS FIXAS) ========================
# =========================================================
SUPABASE_URL = "https://hysrxadnigzqadnlkynq.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh5c3J4YWRuaWd6cWFkbmxreW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MTQwODAsImV4cCI6MjA1OTI5MDA4MH0.RLcu44IvY4X8PLK5BOa_FL5WQ0vJA3p0t80YsGQjTrA"

TABELA_CERTS = "certifica_dfe"
TABELA_NSU   = "nsu_nfs"

BUCKET_STORAGE = "imagens"

PASTA_XML  = "nfse_xml"   # XML solto
PASTA_ZIPS = "notas"      # ZIP do mês

def supabase_headers(is_json: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if is_json:
        h["Content-Type"] = "application/json"
    return h

# =========================================================
# === CONFIGURAÇÕES NFS-e (ADN) ===========================
# =========================================================
ADN_BASE = "https://adn.nfse.gov.br"

START_NSU_DEFAULT = int(os.getenv("START_NSU", "0") or "0")
MAX_NSU_DEFAULT   = int(os.getenv("MAX_NSU", "400") or "400")
INTERVALO_LOOP_SEGUNDOS = int(os.getenv("INTERVALO_LOOP_SEGUNDOS", "90") or "90")

# Concorrência
ADN_WORKERS = int(os.getenv("ADN_WORKERS", "12") or "12")
ADN_BATCH_SIZE = int(os.getenv("ADN_BATCH_SIZE", "60") or "60")

# Parar no 204
STOP_ON_FIRST_204 = (os.getenv("STOP_ON_FIRST_204", "1") or "1").strip().lower() not in ("0", "false", "nao", "não")

# Geração automática do ZIP no começo do mês (dia 1..N)
ZIP_DIA_LIMITE = int(os.getenv("ZIP_DIA_LIMITE", "3") or "3")

# =========================================================
# FUSO HORÁRIO (RONDÔNIA)
# =========================================================
FUSO_RO = ZoneInfo("America/Porto_Velho")

def hoje_ro() -> date:
    return datetime.now(FUSO_RO).date()

def mes_anterior_info() -> Tuple[str, str]:
    """
    Retorna (mes_cod, mes_slug)
      mes_cod  = YYYYMM (ex: 202512)
      mes_slug = YYYY-MM (ex: 2025-12)
    """
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    mes_cod = fim_mes_anterior.strftime("%Y%m")
    mes_slug = fim_mes_anterior.strftime("%Y-%m")
    return mes_cod, mes_slug

def mes_anterior_range_dt() -> Tuple[datetime, datetime]:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)

    data_ini_dt = datetime(inicio_mes_anterior.year, inicio_mes_anterior.month, 1, 0, 0, 0)
    data_fim_dt = datetime(fim_mes_anterior.year, fim_mes_anterior.month, fim_mes_anterior.day, 23, 59, 59)
    return data_ini_dt, data_fim_dt

# =========================================================
# HELPERS
# =========================================================
def somente_numeros(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\D+", "", str(s))

def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v).strip())

def fazer_esta_nao(v: Any) -> bool:
    t = norm_text(v).lower()
    return t in ("nao", "não")

def is_vencido(venc: Any) -> bool:
    if not venc:
        return False
    try:
        s = str(venc)[:10]
        y, m, d = s.split("-")
        vdate = date(int(y), int(m), int(d))
        return vdate < hoje_ro()
    except Exception:
        return False

# =========================================================
# SUPABASE: NSU (salvar/ler último por CNPJ)
# =========================================================
def supabase_get_last_nsu(cnpj: str) -> int:
    cnpj = somente_numeros(cnpj)
    if not cnpj:
        return START_NSU_DEFAULT

    url = f"{SUPABASE_URL}/rest/v1/{TABELA_NSU}"
    params = {"select": "id,cnpj,nsu", "cnpj": f"eq.{cnpj}", "limit": "1", "order": "id.desc"}
    try:
        r = requests.get(url, headers=supabase_headers(), params=params, timeout=20)
        if r.status_code >= 400:
            print(f"   ⚠️ NSU GET falhou ({r.status_code}): {r.text[:200]}")
            return START_NSU_DEFAULT

        rows = r.json() or []
        if not rows:
            return START_NSU_DEFAULT

        nsu_val = rows[0].get("nsu")
        try:
            return int(float(nsu_val))
        except Exception:
            return START_NSU_DEFAULT

    except Exception as e:
        print(f"   ⚠️ Erro lendo NSU no Supabase: {e}")
        return START_NSU_DEFAULT

def supabase_upsert_last_nsu(cnpj: str, nsu: int) -> None:
    """
    Atualiza (PATCH) se existir, senão cria (POST).
    Mantém sempre o maior NSU.
    """
    cnpj = somente_numeros(cnpj)
    if not cnpj:
        return

    url = f"{SUPABASE_URL}/rest/v1/{TABELA_NSU}"
    params = {"select": "id,cnpj,nsu", "cnpj": f"eq.{cnpj}", "limit": "1", "order": "id.desc"}

    try:
        r = requests.get(url, headers=supabase_headers(), params=params, timeout=20)
        if r.status_code >= 400:
            print(f"   ⚠️ NSU GET(para upsert) falhou ({r.status_code}): {r.text[:200]}")
            return

        rows = r.json() or []
        if rows:
            row_id = rows[0].get("id")
            old_nsu = rows[0].get("nsu")
            try:
                old_nsu_int = int(float(old_nsu))
            except Exception:
                old_nsu_int = -1

            new_nsu = max(old_nsu_int, int(nsu))

            patch_url = f"{SUPABASE_URL}/rest/v1/{TABELA_NSU}?id=eq.{row_id}"
            payload = {"cnpj": cnpj, "nsu": float(new_nsu)}
            pr = requests.patch(patch_url, headers=supabase_headers(is_json=True), json=payload, timeout=20)
            if pr.status_code in (200, 204):
                print(f"   ✅ NSU atualizado: cnpj={cnpj} nsu={new_nsu}")
            else:
                print(f"   ⚠️ NSU PATCH falhou ({pr.status_code}): {pr.text[:200]}")
            return

        payload = {"cnpj": cnpj, "nsu": float(int(nsu))}
        pr = requests.post(url, headers=supabase_headers(is_json=True), json=payload, timeout=20)
        if pr.status_code in (200, 201):
            print(f"   ✅ NSU criado: cnpj={cnpj} nsu={int(nsu)}")
        else:
            print(f"   ⚠️ NSU POST falhou ({pr.status_code}): {pr.text[:200]}")

    except Exception as e:
        print(f"   ⚠️ Erro upsert NSU: {e}")

# =========================================================
# SUPABASE: CERTIFICADOS
# =========================================================
def carregar_certificados_validos() -> List[Dict[str, Any]]:
    url = f"{SUPABASE_URL}/rest/v1/{TABELA_CERTS}"
    params = {"select": 'id,pem,key,empresa,codi,user,vencimento,"cnpj/cpf",fazer'}
    print("🔎 Buscando certificados na tabela certifica_dfe...")
    r = requests.get(url, headers=supabase_headers(), params=params, timeout=30)
    r.raise_for_status()
    certs = r.json() or []
    print(f"   ✔ {len(certs)} certificados encontrados.")
    return certs

def criar_arquivos_cert_temp(cert_row: Dict[str, Any]) -> Tuple[str, str, str]:
    pem_b64 = cert_row.get("pem") or ""
    key_b64 = cert_row.get("key") or ""

    pem_bytes = base64.b64decode(pem_b64)
    key_bytes = base64.b64decode(key_b64)

    tmp_dir = tempfile.mkdtemp(prefix="nfse_cert_")
    cert_path = os.path.join(tmp_dir, "cert.pem")
    key_path  = os.path.join(tmp_dir, "key.pem")

    with open(cert_path, "wb") as f:
        f.write(pem_bytes)
    with open(key_path, "wb") as f:
        f.write(key_bytes)

    print(f"   ✔ Certificado temporário: {cert_path}")
    return cert_path, key_path, tmp_dir

# =========================================================
# SUPABASE: STORAGE (LIST/GET/UPLOAD)
# =========================================================
def storage_list(prefix: str, search: Optional[str] = None, limit: int = 1000) -> List[Dict[str, Any]]:
    prefix = prefix.strip("/")

    url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET_STORAGE}"
    headers = supabase_headers(is_json=True)

    payload = {
        "prefix": prefix,
        "limit": int(limit),
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }
    if search:
        payload["search"] = search

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"LIST {r.status_code}: {r.text[:300]}")
    return r.json() or []

def storage_exists(path: str) -> bool:
    path = path.lstrip("/")
    pasta = os.path.dirname(path).replace("\\", "/")
    arquivo = os.path.basename(path)
    try:
        itens = storage_list(prefix=pasta, search=arquivo, limit=200)
        return any((i.get("name") == arquivo) for i in (itens or []))
    except Exception:
        return False

def storage_download(path: str) -> Optional[bytes]:
    path = path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_STORAGE}/{path}"
    r = requests.get(url, headers=supabase_headers(), timeout=180)
    if r.status_code == 200:
        return r.content
    return None

def storage_upload(path: str, content: bytes, content_type: str, upsert: bool = False) -> bool:
    path = path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_STORAGE}/{path}"
    headers = supabase_headers()
    headers["Content-Type"] = content_type
    if upsert:
        headers["x-upsert"] = "true"

    r = requests.post(url, headers=headers, data=content, timeout=300)
    if r.status_code in (200, 201):
        return True

    print(f"   ❌ Upload erro ({r.status_code}) {path}: {r.text[:250]}")
    return False

# =========================================================
# ADN: EXTRAÇÃO XML
# =========================================================
def decode_xml_field(value: str) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None

    if value.lstrip().startswith("<"):
        return value

    try:
        b = base64.b64decode(value, validate=False)
    except Exception:
        return None

    try:
        return gzip.decompress(b).decode("utf-8", errors="replace")
    except Exception:
        try:
            return b.decode("utf-8", errors="replace")
        except Exception:
            return None

def find_xmls(data: Any) -> List[str]:
    xmls: List[str] = []
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, str):
                xml = decode_xml_field(v)
                if xml and xml.strip().startswith("<"):
                    xmls.append(xml)
            else:
                xmls.extend(find_xmls(v))
    elif isinstance(data, list):
        for item in data:
            xmls.extend(find_xmls(item))
    return xmls

def parse_possible_date(texto: str) -> Optional[datetime]:
    if not texto:
        return None
    t = texto.strip()

    if len(t) >= 10 and t[4:5] == "-" and t[7:8] == "-":
        try:
            return datetime.strptime(t[:10], "%Y-%m-%d")
        except Exception:
            pass

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(t, fmt)
        except Exception:
            pass

    return None

def xml_in_period(xml_str: str, data_ini: datetime, data_fim: datetime) -> bool:
    try:
        root = etree.fromstring(xml_str.encode("utf-8", errors="ignore"))
        nodes = root.xpath(
            "//*[contains(translate(local-name(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'data') "
            "or contains(translate(local-name(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'compet')]"
        )
        for n in nodes:
            dt = parse_possible_date((n.text or "").strip())
            if dt and data_ini <= dt <= data_fim:
                return True
    except Exception:
        pass
    return False

def xml_hash_short(xml_str: str) -> str:
    b = xml_str.encode("utf-8", errors="ignore")
    return hashlib.sha1(b).hexdigest()[:16]

# =========================================================
# SESSÃO mTLS (ADN)
# =========================================================
def criar_sessao_adn(cert_path: str, key_path: str) -> requests.Session:
    s = requests.Session()
    s.cert = (cert_path, key_path)
    s.verify = True
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    retries = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

# =========================================================
# CAPTURA: baixar NSUs e salvar XMLs soltos no Storage
# =========================================================
def salvar_xml_solto_storage(cnpj: str, mes_cod: str, nsu: int, idx: int, xml_str: str) -> bool:
    cnpj = somente_numeros(cnpj)
    h = xml_hash_short(xml_str)
    nome = f"{nsu}_{idx:02d}_{h}.xml"
    storage_path = f"{PASTA_XML}/{cnpj}/{mes_cod}/{nome}"

    if storage_exists(storage_path):
        return False

    ok = storage_upload(storage_path, xml_str.encode("utf-8", errors="ignore"), "application/xml", upsert=False)
    if ok:
        print(f"   🧾 XML salvo: {storage_path}")
        return True
    return False

def _extrair_codigo_erro(data_json: dict) -> str:
    try:
        errs = data_json.get("Erros") or []
        if isinstance(errs, list) and errs:
            return str(errs[0].get("Codigo") or "")
    except Exception:
        pass
    return ""

def baixar_e_salvar_xmls_mes_anterior_por_nsu(
    s: requests.Session,
    cnpj: str,
    start_nsu: int,
    max_nsu: int,
    workers: int = ADN_WORKERS,
    batch_size: int = ADN_BATCH_SIZE,
) -> Tuple[int, int, int, bool, str]:
    """
    Retorna:
      xml_salvos, json_ok, last_nsu_testado, nao_avancar_nsu, motivo_nao_avancar
    """
    data_ini, data_fim = mes_anterior_range_dt()
    mes_cod, _mes_slug = mes_anterior_info()

    nsu_atual = int(start_nsu)
    limite = int(start_nsu) + int(max_nsu)

    total_xml_salvos = 0
    total_json_ok = 0
    max_nsu_testado = start_nsu - 1
    stop_all = False

    # quando True e json_ok==0 => não atualiza NSU (mantém antigo)
    nao_avancar_nsu = False
    motivo_nao_avancar = ""

    def fetch_one(nsu: int):
        url = f"{ADN_BASE}/contribuintes/DFe/{nsu}?cnpjConsulta={cnpj}"
        try:
            r = s.get(url, timeout=60)
            return nsu, r, None
        except Exception as e:
            return nsu, None, e

    while nsu_atual < limite and not stop_all:
        fim_lote = min(nsu_atual + batch_size, limite)
        nsus = list(range(nsu_atual, fim_lote))

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(fetch_one, n) for n in nsus]

            for fut in as_completed(futs):
                nsu, r, err = fut.result()

                if nsu > max_nsu_testado:
                    max_nsu_testado = nsu

                if err:
                    print(f"[NSU {nsu}] ERRO REDE: {err}")
                    continue
                if r is None:
                    continue

                # 204 = fim
                if r.status_code == 204:
                    print(f"[NSU {nsu}] Sem conteúdo (204). Encerrando empresa.")
                    if STOP_ON_FIRST_204:
                        stop_all = True
                    continue

                ctype = (r.headers.get("Content-Type") or "").lower()
                body_txt = (r.text or "").strip()

                # ====== TRATAMENTOS QUE DEVEM PULAR E MANTER NSU ANTIGO ======

                # 404 + JSON + StatusProcessamento=NENHUM_DOCUMENTO_LOCALIZADO
                if r.status_code == 404 and "application/json" in ctype:
                    try:
                        data_404 = r.json()
                        st = str(data_404.get("StatusProcessamento") or "").upper().strip()
                        if st == "NENHUM_DOCUMENTO_LOCALIZADO":
                            print(f"[NSU {nsu}] NENHUM_DOCUMENTO_LOCALIZADO (404). Pulando empresa e mantendo NSU antigo.")
                            if total_json_ok == 0:
                                nao_avancar_nsu = True
                                motivo_nao_avancar = "NENHUM_DOCUMENTO_LOCALIZADO"
                            stop_all = True
                            continue
                    except Exception:
                        pass

                # 400 + JSON + StatusProcessamento=REJEICAO/REJEIÇÃO (ex.: E2214)
                if r.status_code == 400 and "application/json" in ctype:
                    try:
                        data_400 = r.json()
                        st = str(data_400.get("StatusProcessamento") or "").upper().strip()
                        if st in ("REJEICAO", "REJEIÇÃO"):
                            cod = _extrair_codigo_erro(data_400)
                            print(f"[NSU {nsu}] REJEICAO (400){(' | Codigo='+cod) if cod else ''}. Pulando empresa e mantendo NSU antigo.")
                            if total_json_ok == 0:
                                nao_avancar_nsu = True
                                motivo_nao_avancar = f"REJEICAO{(':'+cod) if cod else ''}"
                            stop_all = True
                            continue
                    except Exception:
                        pass

                # Outros erros
                if r.status_code >= 400:
                    print(f"[NSU {nsu}] HTTP {r.status_code} | Content-Type={ctype} | Corpo: {body_txt[:220]}")
                    continue

                # Deve ser JSON
                if "application/json" not in ctype:
                    print(f"[NSU {nsu}] Não-JSON. Content-Type={ctype} | Corpo: {body_txt[:200]}")
                    continue

                try:
                    data = r.json()
                except Exception as e:
                    print(f"[NSU {nsu}] JSON inválido ({e}).")
                    continue

                total_json_ok += 1

                xmls = find_xmls(data)
                salvos_nsu = 0

                for i, xml in enumerate(xmls, start=1):
                    if xml_in_period(xml, data_ini, data_fim):
                        if salvar_xml_solto_storage(cnpj=cnpj, mes_cod=mes_cod, nsu=nsu, idx=i, xml_str=xml):
                            total_xml_salvos += 1
                            salvos_nsu += 1

                print(f"[NSU {nsu}] OK - XMLs encontrados: {len(xmls)} | XMLs do mês anterior salvos: {salvos_nsu}")

        nsu_atual = fim_lote

    return total_xml_salvos, total_json_ok, max_nsu_testado, nao_avancar_nsu, motivo_nao_avancar

# =========================================================
# ZIP do mês anterior (a partir dos XMLs soltos no Storage)
# =========================================================
def gerar_zip_mes_anterior_para_empresa(cnpj: str, user: str, codi: Optional[int]) -> None:
    cnpj = somente_numeros(cnpj)
    mes_cod, _mes_slug = mes_anterior_info()

    prefix = f"{PASTA_XML}/{cnpj}/{mes_cod}"
    try:
        itens = storage_list(prefix=prefix, limit=5000)
    except Exception as e:
        print(f"   ⚠️ Falha ao listar XMLs para ZIP ({cnpj} {mes_cod}): {e}")
        return

    nomes = []
    for it in itens or []:
        nm = it.get("name") or ""
        if nm.lower().endswith(".xml"):
            nomes.append(nm)

    if not nomes:
        print(f"   ℹ️ Sem XMLs do mês {mes_cod} no Storage para cnpj={cnpj}. ZIP não gerado.")
        return

    print(f"   📦 Montando ZIP do mês {mes_cod} com {len(nomes)} XMLs (cnpj={cnpj})...")

    buf = tempfile.SpooledTemporaryFile(max_size=80 * 1024 * 1024)
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for nm in nomes:
            obj_path = f"{prefix}/{nm}"
            b = storage_download(obj_path)
            if not b:
                print(f"   ⚠️ Não baixou: {obj_path}")
                continue
            z.writestr(nm, b)

    buf.seek(0)
    zip_bytes = buf.read()

    cod_str = str(codi) if codi is not None else "0"
    email = user or "sem-user"

    zip_name = f"NFSE_{mes_cod}.zip"
    nome_final = f"{mes_cod}-{cod_str}-{cnpj}-{email}-{zip_name}"
    storage_zip_path = f"{PASTA_ZIPS}/{nome_final}"

    ok = storage_upload(storage_zip_path, zip_bytes, "application/zip", upsert=True)
    if ok:
        print(f"   ✅ ZIP atualizado (upsert): {storage_zip_path}")
    else:
        print(f"   ❌ Falha ao enviar ZIP: {storage_zip_path}")

# =========================================================
# Fluxos por empresa
# =========================================================
def fluxo_nfse_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc_raw = cert_row.get("cnpj/cpf") or ""
    cnpj = somente_numeros(doc_raw) or ""

    print("\n\n========================================================")
    print(f"🏢 NFS-e | empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc_raw} | venc: {venc}")
    print("========================================================")

    if not cnpj or len(cnpj) < 11:
        print("⏭️ PULANDO: doc (cnpj/cpf) inválido/ausente.")
        return

    last_saved = supabase_get_last_nsu(cnpj)
    start_nsu = max(0, int(last_saved) + 1)
    max_nsu = MAX_NSU_DEFAULT
    print(f"   🧠 NSU Supabase: last={last_saved} -> start={start_nsu} | max_nsu={max_nsu}")

    try:
        cert_path, key_path, _tmp_dir = criar_arquivos_cert_temp(cert_row)
        s = criar_sessao_adn(cert_path, key_path)
    except Exception as e:
        print("❌ Erro ao criar sessão/cert:", e)
        return

    xml_salvos, json_ok, last_nsu_testado, nao_avancar_nsu, motivo = baixar_e_salvar_xmls_mes_anterior_por_nsu(
        s=s,
        cnpj=cnpj,
        start_nsu=start_nsu,
        max_nsu=max_nsu,
        workers=ADN_WORKERS,
        batch_size=ADN_BATCH_SIZE,
    )

    # Regra: se foi um caso para "pular e manter NSU antigo" e não processou nenhum JSON 200
    if nao_avancar_nsu and json_ok == 0:
        print(f"ℹ️ Mantendo NSU antigo (não atualiza Supabase). Motivo: {motivo or 'NAO_AVANCAR'}")
    else:
        # Só atualiza se teve algum JSON 200 processado (pra não avançar no escuro)
        if json_ok > 0:
            supabase_upsert_last_nsu(cnpj, last_nsu_testado)
        else:
            print("ℹ️ Não atualizou NSU: nenhum JSON 200 processado.")

    print(f"   🧾 XMLs do mês anterior salvos nesta rodada: {xml_salvos} | JSONs OK: {json_ok}")

    # Gera/atualiza ZIP do mês anterior nos primeiros dias do mês (upsert)
    if hoje_ro().day <= ZIP_DIA_LIMITE:
        gerar_zip_mes_anterior_para_empresa(cnpj=cnpj, user=user, codi=codi)

# =========================================================
# MAIN LOOP
# =========================================================
def processar_todas_empresas():
    certs = carregar_certificados_validos()
    if not certs:
        print("⚠️ Nenhum certificado encontrado.")
        return

    hoje = hoje_ro()

    for cert_row in certs:
        empresa = cert_row.get("empresa") or "(sem empresa)"
        user = cert_row.get("user") or ""
        venc = cert_row.get("vencimento")
        fazer = cert_row.get("fazer")

        if fazer_esta_nao(fazer):
            print(f"\n⏭️ PULANDO (fazer='nao'): {empresa} | user: {user}")
            continue

        if is_vencido(venc):
            print(f"\n⏭️ PULANDO (CERT VENCIDO): {empresa} | user: {user} | venc: {venc} | hoje: {hoje.isoformat()}")
            continue

        try:
            fluxo_nfse_para_empresa(cert_row)
        except Exception as e:
            print(f"❌ Erro inesperado em {empresa}: {e}")

def diagnostico_rede_basico():
    host = "adn.nfse.gov.br"
    print("\n[DIAG] Rede: diagnóstico rápido...")
    try:
        ip = socket.gethostbyname(host)
        print(f"[DIAG] DNS OK: {host} -> {ip}")
    except Exception as e:
        print(f"[DIAG] DNS FALHOU: {e}")
        return

    try:
        r = requests.get(f"https://{host}", timeout=(10, 20))
        print(f"[DIAG] GET https://{host} -> {r.status_code}")
    except Exception as e:
        print(f"[DIAG] GET https://{host} falhou: {e}")

# =========================================================
# EXECUÇÃO PRINCIPAL
# =========================================================
if __name__ == "__main__":
    diagnostico_rede_basico()

    while True:
        mes_cod, mes_slug = mes_anterior_info()
        print("\n\n==================== NOVA VARREDURA NFS-e ====================")
        print(f"📅 Data (fuso RO): {hoje_ro().strftime('%d/%m/%Y')} | Mês anterior alvo: {mes_slug} ({mes_cod})")
        try:
            processar_todas_empresas()
        except Exception as e:
            print(f"💥 Erro inesperado no loop: {e}")

        print(f"🕒 Aguardando {INTERVALO_LOOP_SEGUNDOS} segundos...\n")
        time.sleep(INTERVALO_LOOP_SEGUNDOS)
