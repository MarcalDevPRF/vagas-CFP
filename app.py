import io
import os
import json
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, send_file, abort

app = Flask(__name__)
WORKDIR = Path(os.environ.get("LOTACAO_WORKDIR", "/tmp/lotacao"))
WORKDIR.mkdir(parents=True, exist_ok=True)

SAIDA_CSV  = WORKDIR / "resultado_lotacao.csv"
META_JSON  = WORKDIR / "ultima_rodada.json"

# ─────────────────────────────────────────────────────────────────────────────
# 1. UTILITÁRIOS DE MAPEAMENTO
# ─────────────────────────────────────────────────────────────────────────────

def _col(df, *nomes):
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in nomes:
        if n.lower() in mapa: return mapa[n.lower()]
    return None

def carregar_vagas_dict(df):
    unid_col = _col(df, "unidade", "nome_unidade")
    vaga_col = _col(df, "vagas", "quantidade")
    return dict(zip(df[unid_col].astype(str).str.upper(), 
                    pd.to_numeric(df[vaga_col], errors="coerce").fillna(0).astype(int)))

def carregar_respostas_map(df):
    """Lê a aba de respostas atuais, garantindo unicidade por inscrição."""
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição", "Inscrição_aluno")
    if not c_insc:
        raise ValueError("Coluna de inscrição não encontrada em respostas.csv")
    
    # Normalização da chave de busca
    df["insc_norm"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    
    # Trava de segurança: mesmo na aba 'atual', removemos duplicatas se houverem
    df = df.drop_duplicates(subset=["insc_norm"], keep="last")
    
    return df.set_index("insc_norm").to_dict(orient="index")

# ─────────────────────────────────────────────────────────────────────────────
# 2. RECLASSIFICAÇÃO (MÉRITO + COTAS + VAGA ESPELHO)
# ─────────────────────────────────────────────────────────────────────────────

def preparar_listas_por_merito(df):
    df.columns = df.columns.str.strip()
    c_insc = _col(df, "inscricao_aluno", "inscricao")
    c_nome = _col(df, "nome_aluno", "nome")
    c_nota = _col(df, "pontuacao", "pontos", "nota")
    c_nasc = _col(df, "data_nascimento", "nascimento")
    c_conc = _col(df, "concorrencia_aluno", "concorrencia")
    c_sit  = _col(df, "situacao_aluno", "situacao")

    df_p = pd.DataFrame()
    df_p["insc"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    df_p["nome"] = df[c_nome].astype(str).str.strip() if c_nome else "Sem Nome"
    df_p["nota"] = pd.to_numeric(df[c_nota], errors="coerce").fillna(0)
    df_p["nasc"] = pd.to_datetime(df[c_nasc], dayfirst=True, errors="coerce")
    df_p["conc"] = df[c_conc].astype(str).str.upper().fillna("AMPLA")
    df_p["sit"]  = df[c_sit].astype(str).str.upper().fillna("REGULAR")
    
    df_p["nasc_sort"] = df_p["nasc"].fillna(pd.Timestamp("2099-12-31"))
    
    def ordenar(sub_df):
        return sub_df.sort_values(["nota", "nasc_sort"], ascending=[False, True]).reset_index(drop=True)

    return {
        "AMPLA": ordenar(df_p[df_p["conc"] == "AMPLA"]),
        "COTA_NEGRO": ordenar(df_p[df_p["conc"] == "COTA_NEGRO"]),
        "COTA_PCD": ordenar(df_p[df_p["conc"] == "COTA_PCD"])
    }

def gerar_fila_unica(listas):
    ptr = {"AMPLA": 0, "COTA_NEGRO": 0, "COTA_PCD": 0}
    fila_final = []
    pos_ordinal = 1 

    while any(ptr[k] < len(listas[k]) for k in ptr):
        tipo_da_vez = "AMPLA"
        if pos_ordinal == 5 or (pos_ordinal > 21 and (pos_ordinal - 21) % 20 == 0):
            tipo_da_vez = "COTA_PCD"
        elif (pos_ordinal - 3) >= 0 and (pos_ordinal - 3) % 5 == 0:
            tipo_da_vez = "COTA_NEGRO"

        if ptr[tipo_da_vez] >= len(listas[tipo_da_vez]):
            if ptr["AMPLA"] < len(listas["AMPLA"]):
                tipo_da_vez = "AMPLA"
            else:
                tipo_da_vez = next((k for k in ["COTA_NEGRO", "COTA_PCD"] if ptr[k] < len(listas[k])), None)

        if not tipo_da_vez: break

        cand = listas[tipo_da_vez].iloc[ptr[tipo_da_vez]].to_dict()
        ptr[tipo_da_vez] += 1
        cand["posicao_final"] = pos_ordinal
        fila_final.append(cand)

        if cand["sit"] == "REGULAR":
            pos_ordinal += 1

    return pd.DataFrame(fila_final)

# ─────────────────────────────────────────────────────────────────────────────
# 3. MOTOR DE ALOCAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

def processar_alocacao(df_alunos, df_respostas, df_vagas):
    listas = preparar_listas_por_merito(df_alunos)
    df_classificado = gerar_fila_unica(listas)
    
    saldo_vagas = carregar_vagas_dict(df_vagas)
    respostas_map = carregar_respostas_map(df_respostas)
    
    resultados = []

    for _, cand in df_classificado.iterrows():
        insc = str(cand["insc"])
        resp = respostas_map.get(insc)
        
        res = {
            "posicao": cand["posicao_final"],
            "inscricao": cand["insc"],
            "nome": cand["nome"],
            "situacao": cand["sit"],
            "concorrencia": cand["conc"],
            "nota": cand["nota"],
            "unidade": "",
            "status": "NAO_ALOCADO",
            "obs": ""
        }

        if not resp:
            res["status"] = "SEM_ESCOLHA"
            resultados.append(res)
            continue

        # Identificação dinâmica das colunas de opção (opcao_1, opcao_2...)
        opcao_cols = sorted([c for c in resp.keys() if str(c).lower().startswith("opcao_")],
                             key=lambda x: int(x.split('_')[1]) if '_' in x and x.split('_')[1].isdigit() else 0)
        
        opcoes = [str(resp.get(c, "")).strip().upper() for c in opcao_cols 
                  if str(resp.get(c, "")).strip() and str(resp.get(c, "")).upper() not in ("NAN", "NONE", "")]

        acom_conj = str(resp.get("acom_conjuge", "")).upper() in ("SIM", "S")
        conj_reg  = str(resp.get("situacao_conjuge", "")).upper() == "REGULAR"
        
        alocado = False
        for unid in opcoes:
            vagas_disp = saldo_vagas.get(unid, 0)
            vagas_nec = 0
            if cand["sit"] == "REGULAR":
                vagas_nec = 2 if (acom_conj and conj_reg) else 1
            
            if (unid in saldo_vagas) and (vagas_disp >= vagas_nec):
                if cand["sit"] == "REGULAR":
                    saldo_vagas[unid] -= vagas_nec
                
                res["unidade"] = unid
                res["status"] = "ALOCADO"
                res["obs"] = f"Consumiu {vagas_nec} vaga(s)" if cand["sit"]=="REGULAR" else "Subjudice (Espelho)"
                alocado = True
                break
        
        if not alocado:
            res["obs"] = "Vagas esgotadas" if opcoes else "Sem opções válidas"
            
        resultados.append(res)

    return pd.DataFrame(resultados), saldo_vagas

# ─────────────────────────────────────────────────────────────────────────────
# 4. API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        df_a = pd.read_csv(io.BytesIO(request.files["alunos"].read()), encoding="utf-8-sig")
        df_r = pd.read_csv(io.BytesIO(request.files["respostas"].read()), encoding="utf-8-sig")
        df_v = pd.read_csv(io.BytesIO(request.files["vagas"].read()), encoding="utf-8-sig")
        
        df_res, saldo_final = processar_alocacao(df_a, df_r, df_v)
        df_res.to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")
        
        analise = {
            "total": len(df_res),
            "alocados": len(df_res[df_res["status"] == "ALOCADO"]),
            "saldo": saldo_final
        }
        return jsonify({"ok": True, "analise": analise, "data": df_res.head(50).to_dict(orient="records")})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e), "trace": traceback.format_exc()}), 500

@app.route("/resultado/csv", methods=["GET"])
def baixar_csv():
    return send_file(SAIDA_CSV, as_attachment=True, download_name="resultado_lotacao.csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
