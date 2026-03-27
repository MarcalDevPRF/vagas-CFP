import io
import os
import traceback
import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024

# ── HELPERS ──────────────────────────────────────────────────────────────────

def _normalizar_col(s):
    import unicodedata
    s = str(s).strip().replace("\ufeff", "")
    return "".join(c for c in unicodedata.normalize("NFD", s.lower())
                   if unicodedata.category(c) != "Mn")

def _col(df, *candidatos):
    mapa = {_normalizar_col(c): c for c in df.columns}
    for n in candidatos:
        norm = _normalizar_col(n)
        if norm in mapa: return mapa[norm]
    return None

def _norm_str(v):
    return str(v).strip().upper() if pd.notna(v) else ""

# ── 1. CLASSIFICAÇÃO (RELÓGIO ESTABELECIDO) ──────────────────────────────────

def montar_fila_global(df_alunos):
    """
    Intercala conforme: 
    - Negros: 3, 8, 13, 18... 
    - PcD: 5, 21, 41, 61...
    """
    c_insc = _col(df_alunos, "inscricao_aluno", "inscricao")
    c_nome = _col(df_alunos, "nome_aluno", "nome")
    c_nota = _col(df_alunos, "pontuacao", "nota")
    c_nasc = _col(df_alunos, "data_nascimento")
    c_sit  = _col(df_alunos, "situacao_aluno", "situacao")
    c_conc = _col(df_alunos, "concorrencia_aluno", "concorrencia")

    MAPA = {"AMPLA": "AMPLA", "COTA": "COTA_NEGRO", "COTA_NEGRO": "COTA_NEGRO", "PCD": "COTA_PCD", "COTA_PCD": "COTA_PCD"}
    
    df = df_alunos.copy()
    df["_conc_norm"] = df[c_conc].apply(_norm_str).map(MAPA).fillna("AMPLA")
    df["_nota"] = pd.to_numeric(df[c_nota], errors="coerce").fillna(0)
    df["_nasc"] = pd.to_datetime(df[c_nasc], dayfirst=True, errors="coerce")

    def get_fila(cat):
        sub = df[df["_conc_norm"] == cat].sort_values(["_nota", "_nasc"], ascending=[False, True])
        return sub.to_dict("records")

    filas = {k: get_fila(k) for k in ["AMPLA", "COTA_NEGRO", "COTA_PCD"]}
    ptrs = {"AMPLA": 0, "COTA_NEGRO": 0, "COTA_PCD": 0}
    
    resultado = []
    ja_alocados = set()
    
    for pos in range(1, len(df) + 1):
        if pos == 5 or (pos > 5 and (pos - 1) % 20 == 0): tipo_vaga = "COTA_PCD"
        elif pos == 3 or (pos > 3 and (pos - 3) % 5 == 0): tipo_vaga = "COTA_NEGRO"
        else: tipo_vaga = "AMPLA"

        escolhido = None
        while ptrs[tipo_vaga] < len(filas[tipo_vaga]):
            cand = filas[tipo_vaga][ptrs[tipo_vaga]]
            ptrs[tipo_vaga] += 1
            if str(cand[c_insc]) not in ja_alocados:
                escolhido = cand; break
        
        if not escolhido:
            while ptrs["AMPLA"] < len(filas["AMPLA"]):
                cand = filas["AMPLA"][ptrs["AMPLA"]]
                ptrs["AMPLA"] += 1
                if str(cand[c_insc]) not in ja_alocados:
                    escolhido = cand; break

        if escolhido:
            ja_alocados.add(str(escolhido[c_insc]))
            resultado.append({
                "insc": str(escolhido[c_insc]),
                "nome": str(escolhido[c_nome]),
                "conc_origem": escolhido["_conc_norm"],
                "situacao": _norm_str(escolhido[c_sit])
            })
    return resultado

# ── 2. ALOCAÇÃO COM REGRA DE CÔNJUGE E SUBJUDICE ────────────────────────────

def processar_lotacao(fila_global, resp_map, opcao_cols, saldo_vagas):
    regulares = []
    subjudices = []
    
    # Criar um mapa rápido para consultar situação de qualquer inscrito
    situacao_map = {insc: resp.get("situacao_aluno", "REGULAR").upper() for insc, resp in resp_map.items()}

    for i, cand in enumerate(fila_global):
        insc = cand["insc"]
        resp = resp_map.get(insc, {})
        opcoes = [_norm_str(resp.get(c)) for c in opcao_cols if _norm_str(resp.get(c))]
        
        # Identificação de Cônjuge
        acom_conj = _norm_str(resp.get("acom_conjuge")) in ("SIM", "S")
        insc_conj = _norm_str(resp.get("inscricao_conjuge"))
        is_sub = cand["situacao"] == "SUBJUDICE"

        # Regra de Custo de Vaga:
        # Se tem cônjuge e o cônjuge NÃO é subjudice, precisa de 2 vagas.
        # Se o cônjuge FOR subjudice, o titular só consome 1 vaga (a dele).
        custo = 1
        if not is_sub and acom_conj:
            sit_conj = situacao_map.get(insc_conj, "REGULAR")
            if sit_conj != "SUBJUDICE":
                custo = 2

        # Subjudices nunca consomem saldo (custo 0)
        if is_sub: custo = 0

        unidade_destino = ""
        ordem_opt = ""

        # 1. Tentar Opções
        for idx, u in enumerate(opcoes):
            if u in saldo_vagas and (is_sub or saldo_vagas[u] >= custo):
                if not is_sub: saldo_vagas[u] -= custo
                unidade_destino = u
                ordem_opt = f"{idx + 1}ª Opção"
                break
        
        # 2. Alocação Forçada (Garante 100% de alocação para Regulares)
        if not unidade_destino:
            if not saldo_vagas:
                unidade_destino = "SEM_VAGA"
                ordem_opt = "Sem vaga disponível"
            else:
                unid_reserva = sorted(saldo_vagas.keys(), key=lambda k: saldo_vagas[k], reverse=True)[0]
                if not is_sub: saldo_vagas[unid_reserva] -= custo
                unidade_destino = unid_reserva
                ordem_opt = "Ex Officio"

        # Registro Principal
        registro = {
            "Classificação Final": i + 1,
            "Concorrência": cand["conc_origem"],
            "Inscrição": insc,
            "Nome": cand["nome"],
            "Papel": "Candidato",
            "Lotação": unidade_destino,
            "Ordem de Opção": ordem_opt
        }

        if is_sub:
            subjudices.append(registro)
        else:
            regulares.append(registro)
            # Se alocou com cônjuge, insere o acompanhante na tabela
            if acom_conj:
                regulares.append({
                    "Classificação Final": i + 1,
                    "Concorrência": "ACOMPANHANTE",
                    "Inscrição": f"AC-{insc_conj}" if insc_conj else "S/INC",
                    "Nome": f"ACOMPANHANTE DE {cand['nome']}",
                    "Papel": "Acompanhante",
                    "Lotação": unidade_destino,
                    "Ordem de Opção": "Vaga Vinculada"
                })

    return regulares, subjudices

# ── 3. FLASK ROUTES ──────────────────────────────────────────────────────────

@app.route("/", methods=["GET", "HEAD"])
def health():
    return jsonify({"ok": True, "status": "online"})

@app.route("/health", methods=["GET", "HEAD"])
def health_check():
    return jsonify({"ok": True, "status": "online"})

@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        data = request.get_json(force=True, silent=True)
        if data:
            # veio JSON
            csv_alunos    = data.get("csv_alunos", "")
            csv_respostas = data.get("csv_respostas", "")
            csv_vagas     = data.get("csv_vagas", "")
        else:
            # veio multipart/form-data (CSVs enviados como arquivos)
            csv_alunos    = request.files.get("alunos").read().decode("utf-8-sig")
            csv_respostas = request.files.get("respostas").read().decode("utf-8-sig")
            csv_vagas     = request.files.get("vagas").read().decode("utf-8-sig")
        df_a = pd.read_csv(io.StringIO(csv_alunos), encoding="utf-8-sig")
        df_r = pd.read_csv(io.StringIO(csv_respostas), encoding="utf-8-sig")
        df_v = pd.read_csv(io.StringIO(csv_vagas), encoding="utf-8-sig")

        # Preparar dados
        c_unid = _col(df_v, "unidade", "nome_unidade")
        c_qtd  = _col(df_v, "vagas", "quantidade")

        if c_unid is None or c_qtd is None:
            return jsonify({
                "ok": False,
                "erro": (
                    f"CSV de vagas não tem as colunas esperadas.\n"
                    f"  Coluna unidade encontrada: {c_unid!r} (esperava 'unidade' ou 'nome_unidade')\n"
                    f"  Coluna vagas encontrada:   {c_qtd!r} (esperava 'vagas' ou 'quantidade')\n"
                    f"  Colunas presentes: {list(df_v.columns)}"
                )
            })

        saldo = { _norm_str(r[c_unid]): int(pd.to_numeric(r[c_qtd], errors="coerce") or 0)
                 for _, r in df_v.iterrows() if _norm_str(r[c_unid]) }

        if not saldo:
            return jsonify({
                "ok": False,
                "erro": (
                    f"Nenhuma unidade válida encontrada no CSV de vagas.\n"
                    f"  Coluna usada para unidade: {c_unid!r}\n"
                    f"  Coluna usada para vagas:   {c_qtd!r}\n"
                    f"  Primeiras linhas: {df_v.head(3).to_dict('records')}"
                )
            })

        # Criar mapa de respostas
        c_insc_r = _col(df_r, "inscricao_aluno", "inscricao")
        df_r["_insc"] = df_r[c_insc_r].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
        opt_cols = sorted([c for c in df_r.columns if c.lower().startswith("opcao_")],
                          key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
        resp_map = df_r.set_index("_insc").to_dict(orient="index")

        # Executar
        fila = montar_fila_global(df_a)
        reg, sub = processar_lotacao(fila, resp_map, opt_cols, saldo)

        return jsonify({
            "ok": True,
            "tabela_regulares": reg,
            "tabela_subjudices": sub
        })
    except Exception:
        return jsonify({"ok": False, "erro": traceback.format_exc()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
