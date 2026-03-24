import io
import os
import json
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, send_file, abort

app = Flask(__name__)
WORKDIR = Path("/tmp/lotacao")
WORKDIR.mkdir(parents=True, exist_ok=True)

SAIDA_CSV  = WORKDIR / "resultado_lotacao.csv"

# --- AUXILIARES ---
def _col(df, *nomes):
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in nomes:
        if n.lower() in mapa: return mapa[n.lower()]
    return None

def carregar_vagas_dict(df):
    unid_col = _col(df, "unidade", "nome_unidade")
    vaga_col = _col(df, "vagas", "quantidade")
    if not unid_col or not vaga_col: return {}
    return dict(zip(df[unid_col].astype(str).str.upper(), 
                    pd.to_numeric(df[vaga_col], errors="coerce").fillna(0).astype(int)))

def carregar_respostas_map(df):
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição")
    if not c_insc: return {}
    # Normalização e remoção de duplicatas (Ponto crucial para evitar Erro 500)
    df["insc_norm"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    df = df.drop_duplicates(subset=["insc_norm"], keep="last")
    return df.set_index("insc_norm").to_dict(orient="index")

# --- LÓGICA DE FILA ÚNICA ---
def preparar_listas(df):
    c_insc = _col(df, "inscricao_aluno", "inscricao")
    c_nota = _col(df, "pontuacao", "pontos", "nota")
    c_nasc = _col(df, "data_nascimento", "nascimento")
    c_conc = _col(df, "concorrencia_aluno", "concorrencia")
    c_sit  = _col(df, "situacao_aluno", "situacao")

    df_p = pd.DataFrame()
    df_p["insc"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    df_p["nome"] = df[_col(df, "nome_aluno", "nome") or df.columns[0]].astype(str)
    df_p["nota"] = pd.to_numeric(df[c_nota], errors="coerce").fillna(0)
    df_p["nasc"] = pd.to_datetime(df[c_nasc], dayfirst=True, errors="coerce")
    df_p["conc"] = df[c_conc].astype(str).str.upper().fillna("AMPLA")
    df_p["sit"]  = df[c_sit].astype(str).str.upper().fillna("REGULAR")
    df_p["nasc_sort"] = df_p["nasc"].fillna(pd.Timestamp("2099-12-31"))
    
    def ordenar(sub): return sub.sort_values(["nota", "nasc_sort"], ascending=[False, True]).reset_index(drop=True)

    return {
        "AMPLA": ordenar(df_p[df_p["conc"] == "AMPLA"]),
        "COTA_NEGRO": ordenar(df_p[df_p["conc"] == "COTA_NEGRO"]),
        "COTA_PCD": ordenar(df_p[df_p["conc"] == "COTA_PCD"])
    }

def gerar_fila(listas):
    ptr = {"AMPLA": 0, "COTA_NEGRO": 0, "COTA_PCD": 0}
    fila = []
    pos_ord = 1
    while any(ptr[k] < len(listas[k]) for k in ptr):
        vez = "AMPLA"
        if pos_ord == 5 or (pos_ord > 21 and (pos_ord - 21) % 20 == 0): vez = "COTA_PCD"
        elif (pos_ord - 3) >= 0 and (pos_ord - 3) % 5 == 0: vez = "COTA_NEGRO"

        if ptr[vez] >= len(listas[vez]):
            vez = "AMPLA" if ptr["AMPLA"] < len(listas["AMPLA"]) else next((k for k in ptr if ptr[k]<len(listas[k])), None)
        
        if not vez: break
        cand = listas[vez].iloc[ptr[vez]].to_dict()
        ptr[vez] += 1
        cand["posicao_final"] = pos_ord
        fila.append(cand)
        if cand["sit"] == "REGULAR": pos_ord += 1
    return pd.DataFrame(fila)

# --- EXECUÇÃO ---
@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        # Lendo arquivos com tratamento de erro de encoding
        df_a = pd.read_csv(request.files["alunos"], encoding="utf-8-sig")
        df_r = pd.read_csv(request.files["respostas"], encoding="utf-8-sig")
        df_v = pd.read_csv(request.files["vagas"], encoding="utf-8-sig")
        
        listas = preparar_listas(df_a)
        df_fila = gerar_fila(listas)
        vagas = carregar_vagas_dict(df_v)
        resp_map = carregar_respostas_map(df_r)
        
        resultados = []
        for _, c in df_fila.iterrows():
            r = resp_map.get(str(c["insc"]), {})
            res = {"posicao": c["posicao_final"], "insc": c["insc"], "nome": c["nome"], "status": "NAO_ALOCADO", "unidade": ""}
            
            # Busca dinâmica de opções
            opcoes = [str(r.get(f"opcao_{i}", "")).strip().upper() for i in range(1, 11) if r.get(f"opcao_{i}")]
            
            for u in opcoes:
                if u in vagas:
                    nec = 1 if c["sit"] == "REGULAR" else 0
                    # Lógica de cônjuge simplificada para evitar 502 por lentidão
                    if c["sit"] == "REGULAR" and str(r.get("acom_conjuge")).upper() in ("SIM","S") and str(r.get("situacao_conjuge")).upper() == "REGULAR":
                        nec = 2
                    
                    if vagas[u] >= nec:
                        if c["sit"] == "REGULAR": vagas[u] -= nec
                        res.update({"unidade": u, "status": "ALOCADO"})
                        break
            resultados.append(res)

        pd.DataFrame(resultados).to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")
        return jsonify({"ok": True, "msg": "Processado"})
    except Exception:
        return jsonify({"ok": False, "trace": traceback.format_exc()}), 500

@app.route("/resultado/csv")
def dload(): return send_file(SAIDA_CSV, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
