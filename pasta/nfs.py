# -*- coding: utf-8 -*-
import os
import re
import time
import json
import base64
import gzip
import socket
import zipfile
import tempfile
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
TABELA_NSU   = "nsu_nfs"           # <<< sua tabela
BUCKET_IMAGENS = "imagens"
PASTA_NOTAS = "notas"

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

# Parada ao achar 204 (fim do disponível)
STOP_ON_FIRST_204 = (os.getenv("STOP_ON_FIRST_204", "1") or "1").strip().lower() not in ("0", "false", "nao", "não")

# =========================================================
# FUSO HORÁRIO (RONDÔNIA)
# =========================================================
FUSO_RO = ZoneInfo("America/Porto_Velho")

def hoje_ro() -> date:
    return datetime.now(FUSO_RO).date()

def mes_anterior_codigo() -> str:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    return fim_mes_anterior.strftime("%Y%m")

def mes_anterior_range_dt() -> Tuple[datetime, datetime]:
    hoje = hoje_ro()
    inicio_mes_atual = hoje.replace(day=1)
    fim_mes_anterior = inicio_mes_atual - timedelta(days=1)
    inicio_mes_anterior = fim_mes_anterior.replace(day=1)

    data_ini_dt = datetime(inicio_mes_anterior.year, inicio_mes_anterior.month, inicio_mes_anterior.day, 0, 0, 0)
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
                print(f"   ✅ NSU atualizado no Supabase: cnpj={cnpj} nsu={new_nsu}")
            else:
                print(f"   ⚠️ NSU PATCH falhou ({pr.status_code}): {pr.text[:200]}")
            return

        payload = {"cnpj": cnpj, "nsu": float(int(nsu))}
        pr = requests.post(url, headers=supabase_headers(is_json=True), json=payload, timeout=20)
        if pr.status_code in (200, 201):
            print(f"   ✅ NSU criado no Supabase: cnpj={cnpj} nsu={int(nsu)}")
        else:
            print(f"   ⚠️ NSU POST falhou ({pr.status_code}): {pr.text[:200]}")

    except Exception as e:
        print(f"   ⚠️ Erro upsert NSU no Supabase: {e}")

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
# SUPABASE: STORAGE
# =========================================================
def arquivo_ja_existe_no_storage(storage_path: str) -> bool:
    storage_path = storage_path.lstrip("/")
    pasta = os.path.dirname(storage_path).replace("\\", "/")
    arquivo = os.path.basename(storage_path)

    url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET_IMAGENS}"
    headers = supabase_headers(is_json=True)

    payload = {
        "prefix": pasta,
        "search": arquivo,
        "limit": 100,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code != 200:
            print(f"   ⚠️ LIST retornou {r.status_code} ao checar {storage_path}: {r.text[:200]}")
            return False

        itens = r.json() or []
        existe = any((i.get("name") == arquivo) for i in itens)
        if existe:
            print(f"   ⚠️ Já existe no storage: {storage_path}")
        return existe

    except Exception as e:
        print(f"   ⚠️ Erro ao checar existência no storage (LIST) ({storage_path}): {e}")
        return False

def upload_para_storage(storage_path: str, conteudo: bytes, content_type: str = "application/zip") -> bool:
    storage_path = storage_path.lstrip("/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET_IMAGENS}/{storage_path}"
    headers = supabase_headers()
    headers["Content-Type"] = content_type

    try:
        r = requests.post(url, headers=headers, data=conteudo, timeout=180)
        if r.status_code in (200, 201):
            print(f"   🎉 Upload OK: {storage_path}")
            return True
        print(f"   ❌ Upload erro ({r.status_code}) {storage_path}: {r.text[:400]}")
        return False
    except Exception as e:
        print(f"   ❌ Erro upload ({storage_path}): {e}")
        return False

def montar_nome_final_arquivo(
    base_name: str,
    user: str,
    codi: Optional[int],
    mes_cod: str,
    doc: str,
) -> str:
    doc_clean = somente_numeros(doc) or "sem-doc"
    cod_str = str(codi) if codi is not None else "0"
    email = user or "sem-user"
    return f"{mes_cod}-{cod_str}-{doc_clean}-{email}-{base_name}"

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
# ZIP BUILDER
# =========================================================
def zipar_pasta_em_memoria(pasta_local: str) -> bytes:
    buf = tempfile.SpooledTemporaryFile(max_size=50 * 1024 * 1024)
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _dirs, files in os.walk(pasta_local):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, pasta_local).replace("\\", "/")
                z.write(full, rel)
    buf.seek(0)
    return buf.read()

# =========================================================
# DOWNLOAD NFS-e (CONCORRENTE EM LOTES)
#  - Trata 204 como fim
#  - Trata 404 com StatusProcessamento=NENHUM_DOCUMENTO_LOCALIZADO como "sem documento" (para e NÃO avança NSU)
# =========================================================
def baixar_nfse_mes_anterior_para_pasta(
    s: requests.Session,
    cnpj: str,
    pasta_saida: str,
    start_nsu: int,
    max_nsu: int,
    workers: int = ADN_WORKERS,
    batch_size: int = ADN_BATCH_SIZE,
) -> Tuple[int, int, int, bool]:
    """
    Retorna:
      total_xml_salvos,
      total_json_ok (200 json processado),
      last_nsu_para_salvar (quando fizer sentido),
      no_docs (True quando cair em NENHUM_DOCUMENTO_LOCALIZADO sem processar nenhum JSON)
    """
    data_ini, data_fim = mes_anterior_range_dt()

    nsu_atual = int(start_nsu)
    limite = int(start_nsu) + int(max_nsu)

    total_salvos = 0
    total_json_ok = 0

    stop_all = False
    max_nsu_testado = start_nsu - 1

    # se cair no "nenhum documento localizado" sem ter processado nenhum json válido,
    # consideramos que NÃO devemos avançar o NSU (fica o antigo)
    no_docs = False

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

                # ---- FIM por 204
                if r.status_code == 204:
                    print(f"[NSU {nsu}] Sem conteúdo (204). Encerrando empresa.")
                    stop_all = True
                    continue

                ctype = (r.headers.get("Content-Type") or "").lower()
                body_txt = (r.text or "").strip()

                # ---- CASO DO SEU LOG: 404 com JSON e StatusProcessamento=NENHUM_DOCUMENTO_LOCALIZADO
                if r.status_code == 404 and "application/json" in ctype:
                    try:
                        data_404 = r.json()
                        st = str(data_404.get("StatusProcessamento") or "").upper().strip()
                        if st == "NENHUM_DOCUMENTO_LOCALIZADO":
                            print(f"[NSU {nsu}] NENHUM_DOCUMENTO_LOCALIZADO (404). Pulando empresa e mantendo NSU antigo.")
                            # se ainda não tivemos nenhum JSON 200, marcamos no_docs para NÃO atualizar NSU
                            if total_json_ok == 0:
                                no_docs = True
                            stop_all = True
                            continue
                    except Exception:
                        pass  # cai no tratamento normal abaixo

                # ---- Outros erros
                if r.status_code >= 400:
                    print(f"[NSU {nsu}] HTTP {r.status_code} | Content-Type={ctype} | Corpo: {body_txt[:220]}")
                    continue

                # ---- Deve ser JSON
                if "application/json" not in ctype:
                    print(f"[NSU {nsu}] Não-JSON. Content-Type={ctype} | Corpo: {body_txt[:200]}")
                    continue

                try:
                    data = r.json()
                except Exception as e:
                    print(f"[NSU {nsu}] JSON inválido ({e}).")
                    continue

                total_json_ok += 1

                # salva bruto sempre (se quiser deixar mais leve, eu removo depois)
                raw_fn = os.path.join(pasta_saida, f"nsu_{nsu}_raw.json")
                try:
                    with open(raw_fn, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    print(f"[NSU {nsu}] Falha ao salvar JSON bruto: {e}")

                xmls = find_xmls(data)
                salvos_nsu = 0

                for i, xml in enumerate(xmls, start=1):
                    if xml_in_period(xml, data_ini, data_fim):
                        total_salvos += 1
                        salvos_nsu += 1
                        nome = f"NFS-e_{nsu}_{i}_{total_salvos}.xml"
                        xml_path = os.path.join(pasta_saida, nome)
                        try:
                            with open(xml_path, "w", encoding="utf-8") as f:
                                f.write(xml)
                        except Exception as e:
                            print(f"[NSU {nsu}] Erro salvando XML {nome}: {e}")

                print(f"[NSU {nsu}] OK - XMLs encontrados: {len(xmls)} | XMLs salvos no período: {salvos_nsu}")

        nsu_atual = fim_lote

        # se STOP_ON_FIRST_204=1 e já marcou stop_all, o while termina naturalmente
        # (no_docs também encerra)

    # last_nsu_para_salvar:
    # - se no_docs=True e total_json_ok==0 => NÃO queremos avançar, então retornamos start_nsu-1 (vai ser ignorado no fluxo)
    # - caso contrário, avançamos para o maior NSU testado
    if no_docs and total_json_ok == 0:
        last_nsu_para_salvar = start_nsu - 1
    else:
        last_nsu_para_salvar = max_nsu_testado

    return total_salvos, total_json_ok, int(last_nsu_para_salvar), bool(no_docs)

# =========================================================
# FLUXO NFS-e (ADN)
# =========================================================
def fluxo_nfse_para_empresa(cert_row: Dict[str, Any]):
    empresa = cert_row.get("empresa") or ""
    user = cert_row.get("user") or ""
    codi = cert_row.get("codi")
    venc = cert_row.get("vencimento")
    doc_raw = cert_row.get("cnpj/cpf") or ""
    doc_alvo = somente_numeros(doc_raw) or ""

    print("\n\n========================================================")
    print(f"🏢 NFS-e | empresa: {empresa} | user: {user} | codi: {codi} | doc: {doc_raw} | venc: {venc}")
    print("========================================================")

    if not doc_alvo or len(doc_alvo) < 11:
        print("⏭️ PULANDO: doc (cnpj/cpf) inválido/ausente.")
        return

    # pega NSU salvo e começa no próximo
    last_saved = supabase_get_last_nsu(doc_alvo)
    start_nsu = max(0, int(last_saved) + 1)
    max_nsu = MAX_NSU_DEFAULT

    print(f"   🧠 NSU Supabase: last={last_saved} -> start={start_nsu} | max_nsu={max_nsu}")

    try:
        cert_path, key_path, _tmp_dir = criar_arquivos_cert_temp(cert_row)
        s = criar_sessao_adn(cert_path, key_path)
    except Exception as e:
        print("❌ Erro ao criar sessão/cert:", e)
        return

    work_dir = tempfile.mkdtemp(prefix="nfse_adn_")
    print(f"   📁 Pasta temporária: {work_dir}")
    print(f"   ⚙️ ADN: workers={ADN_WORKERS} | batch_size={ADN_BATCH_SIZE} | stop204={int(STOP_ON_FIRST_204)}")

    total_xml, total_json_ok, last_nsu_para_salvar, no_docs = baixar_nfse_mes_anterior_para_pasta(
        s=s,
        cnpj=doc_alvo,
        pasta_saida=work_dir,
        start_nsu=start_nsu,
        max_nsu=max_nsu,
        workers=ADN_WORKERS,
        batch_size=ADN_BATCH_SIZE,
    )

    # ✅ REGRA DO JOSIMAR:
    # Se veio "NENHUM_DOCUMENTO_LOCALIZADO" sem processar nenhum JSON 200,
    # NÃO atualiza NSU (fica o antigo).
    if no_docs and total_json_ok == 0:
        print("ℹ️ Nenhum documento localizado. Mantendo NSU antigo (não atualiza Supabase).")
    else:
        # se processou algo (ou avançou normal), atualiza para o maior NSU testado
        if total_json_ok > 0:
            supabase_upsert_last_nsu(doc_alvo, last_nsu_para_salvar)
        else:
            print("ℹ️ Não atualizou NSU: nenhum JSON válido (200) processado.")

    # Se não processou nada, pula
    if total_json_ok == 0:
        print("⚠️ Nada processado no ADN (nenhum JSON 200). Pulando empresa.")
        return

    # ✅ REGRA: se foi apenas JSON (nenhum XML do mês anterior), não cria ZIP
    if total_xml == 0:
        print("ℹ️ Não houve XMLs do mês anterior. Não cria ZIP / não envia.")
        return

    # Se tiver XML do período, envia ZIP (JSON + XML)
    tem_arquivos = False
    for _root, _dirs, files in os.walk(work_dir):
        if files:
            tem_arquivos = True
            break

    if not tem_arquivos:
        print("⚠️ Pasta vazia. Não envia ZIP.")
        return

    mes_cod = mes_anterior_codigo()
    base_name = f"NFSE_{mes_cod}.zip"
    nome_final = montar_nome_final_arquivo(
        base_name=base_name,
        user=user,
        codi=codi,
        mes_cod=mes_cod,
        doc=doc_alvo or doc_raw,
    )
    storage_path = f"{PASTA_NOTAS}/{nome_final}"

    if arquivo_ja_existe_no_storage(storage_path):
        print(f"⤵ Já existe no storage. Não reenvia: {storage_path}")
        return

    try:
        zip_bytes = zipar_pasta_em_memoria(work_dir)
        print(f"   📦 ZIP pronto ({len(zip_bytes)/1024:.1f} KB) | XMLs mês anterior: {total_xml} | JSONs OK: {total_json_ok}")
    except Exception as e:
        print("❌ Falha ao zipar:", e)
        return

    ok = upload_para_storage(storage_path, zip_bytes, content_type="application/zip")
    if ok:
        print(f"✅ NFS-e enviado: {storage_path}")
    else:
        print(f"❌ Falhou upload NFS-e: {storage_path}")

# =========================================================
# MAIN LOOP NFS-e
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
        print("\n\n==================== NOVA VARREDURA NFS-e ====================")
        print(f"📅 Data (fuso RO): {hoje_ro().strftime('%d/%m/%Y')}")
        try:
            processar_todas_empresas()
        except Exception as e:
            print(f"💥 Erro inesperado no loop: {e}")

        print(f"🕒 Aguardando {INTERVALO_LOOP_SEGUNDOS} segundos...\n")
        time.sleep(INTERVALO_LOOP_SEGUNDOS)
