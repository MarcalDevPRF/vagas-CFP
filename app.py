import io
import os
import json
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, send_file, abort

# Configurações de Ambiente
app = Flask(__name__)
WORKDIR = Path(os.environ.get("LOTACAO_WORKDIR", "/tmp/lotacao"))
WORKDIR.mkdir(parents=True, exist_ok=True)

SAIDA_CSV  = WORKDIR / "resultado_lotacao.csv"
META_JSON  = WORKDIR / "ultima_rodada.json"

# ─────────────────────────────────────────────────────────────────────────────
# 1. UTILITÁRIOS DE MAPEAMENTO E CARGA
# ─────────────────────────────────────────────────────────────────────────────

def _col(df, *nomes):
    """Localiza o nome real da coluna no CSV independente de maiúsculas/espaços."""
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in nomes:
        if n.lower() in mapa: return mapa[n.lower()]
    return None

def carregar_vagas_dict(df):
    unid_col = _col(df, "unidade", "nome_unidade")
    vaga_col = _col(df, "vagas", "quantidade", "qtd")
    if not unid_col or not vaga_col:
        raise ValueError("Colunas 'unidade' ou 'vagas' não encontradas em vagas.csv")
    return dict(zip(df[unid_col].astype(str).str.upper(), 
                    pd.to_numeric(df[vaga_col], errors="coerce").fillna(0).astype(int)))

def carregar_respostas_map(df):
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição", "Inscrição_aluno")
    if not c_insc:
        raise ValueError("Coluna de inscrição não encontrada em respostas.csv")
    # Limpeza para evitar erro de .0 em números de inscrição
    df["insc_norm"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    return df.set_index("insc_norm").to_dict(orient="index")

# ─────────────────────────────────────────────────────────────────────────────
# 2. LÓGICA DE RECLASSIFICAÇÃO (A REGRA DO MÉRITO + COTAS)
# ─────────────────────────────────────────────────────────────────────────────

def preparar_listas_por_merito(df):
    """Separa os alunos em baldes e ordena cada um por Nota e Idade."""
    df.columns = df.columns.str.strip()
    
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição")
    c_nome = _col(df, "nome_aluno", "nome")
    c_nota = _col(df, "pontuacao", "pontos", "nota")
    c_nasc = _col(df, "data_nascimento", "nascimento")
    c_conc = _col(df, "concorrencia_aluno", "concorrencia")
    c_sit  = _col(df, "situacao_aluno", "situacao")

    # Criar colunas padronizadas para o processamento
    df_p = pd.DataFrame()
    df_p["insc"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    df_p["nome"] = df[c_nome].astype(str).str.strip() if c_nome else "Sem Nome"
    df_p["nota"] = pd.to_numeric(df[c_nota], errors="coerce").fillna(0)
    df_p["nasc"] = pd.to_datetime(df[c_nasc], dayfirst=True, errors="coerce")
    df_p["conc"] = df[c_conc].astype(str).str.upper().fillna("AMPLA")
    df_p["sit"]  = df[c_sit].astype(str).str.upper().fillna("REGULAR")
    
    # Ordenação: Nota (Desc) e Nascimento (Asc - mais velho primeiro)
    df_p["nasc_sort"] = df_p["nasc"].fillna(pd.Timestamp("2099-12-31"))
    
    def ordenar(sub_df):
        return sub_df.sort_values(["nota", "nasc_sort"], ascending=[False, True]).reset_index(drop=True)

    return {
        "AMPLA": ordenar(df_p[df_p["conc"] == "AMPLA"]),
        "COTA_NEGRO": ordenar(df_p[df_p["conc"] == "COTA_NEGRO"]),
        "COTA_PCD": ordenar(df_p[df_p["conc"] == "COTA_PCD"])
    }

def gerar_fila_unica(listas):
    """Aplica a regra de alternância e a Vaga Espelho para Subjudice."""
    ptr = {"AMPLA": 0, "COTA_NEGRO": 0, "COTA_PCD": 0}
    fila_final = []
    pos_ordinal = 1 # Este é o índice que o Regular ocupa.

    while any(ptr[k] < len(listas[k]) for k in ptr):
        # Gatilhos de Cota
        tipo_da_vez = "AMPLA"
        if pos_ordinal == 5 or (pos_ordinal > 21 and (pos_ordinal - 21) % 20 == 0):
            tipo_da_vez = "COTA_PCD"
        elif (pos_ordinal - 3) >= 0 and (pos_ordinal - 3) % 5 == 0:
            tipo_da_vez = "COTA_NEGRO"

        # Fallback: Se a cota esgotou, vai para Ampla. Se tudo esgotou, pega o que houver.
        if ptr[tipo_da_vez] >= len(listas[tipo_da_vez]):
            if ptr["AMPLA"] < len(listas["AMPLA"]):
                tipo_da_vez = "AMPLA"
            else:
                tipo_da_vez = next((k for k in ["COTA_NEGRO", "COTA_PCD"] if ptr[k] < len(listas[k])), None)

        if not tipo_da_vez: break

        # Extração do candidato por Mérito
        cand = listas[tipo_da_vez].iloc[ptr[tipo_da_vez]].to_dict()
        ptr[tipo_da_vez] += 1
        
        # Atribuição da posição
        cand["posicao_final"] = pos_ordinal
        fila_final.append(cand)

        # REGRA VAGA ESPELHO: 
        # Somente incrementamos a posição se o candidato for REGULAR.
        # Se for SUBJUDICE, o próximo da fila terá a mesma posição ordinal (espelhada).
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
    vagas_orig = dict(saldo_vagas)
    respostas_map = carregar_respostas_map(df_respostas)
    opcao_cols = sorted([c for c in df_respostas.columns if c.lower().startswith("opcao_")],
                         key=lambda x: int(x.split('_')[1]) if '_' in x and x.split('_')[1].isdigit() else 0)

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

        # Regra de Cônjuge
        acom_conj = str(resp.get("acom_conjuge", "")).upper() in ("SIM", "S")
        conj_reg  = str(resp.get("situacao_conjuge", "")).upper() == "REGULAR"
        
        opcoes = [str(resp.get(c, "")).strip().upper() for c in opcao_cols if str(resp.get(c, "")).strip()]
        
        alocado = False
        for unid in opcoes:
            vagas_disp = saldo_vagas.get(unid, 0)
            
            # Cálculo de consumo (Subjudice não abate vaga do saldo principal)
            vagas_necessarias = 0
            if cand["sit"] == "REGULAR":
                vagas_necessarias = 2 if (acom_conj and conj_reg) else 1
            
            if (unid in saldo_vagas) and (vagas_disp >= vagas_necessarias):
                if cand["sit"] == "REGULAR":
                    saldo_vagas[unid] -= vagas_necessarias
                
                res["unidade"] = unid
                res["status"] = "ALOCADO"
                res["obs"] = f"Consumiu {vagas_necessarias} vaga(s)" if cand["sit"] == "REGULAR" else "Subjudice (Vaga Extra/Espelho)"
                alocado = True
                break
        
        if not alocado:
            res["obs"] = "Vagas esgotadas nas opções" if opcoes else "Nenhuma opção válida"
            
        resultados.append(res)

    return pd.DataFrame(resultados), saldo_vagas

# ─────────────────────────────────────────────────────────────────────────────
# 4. ENDPOINTS FLASK
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "online", "timestamp": datetime.now().isoformat()})

@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        # Leitura segura dos arquivos recebidos
        if not all(k in request.files for k in ("alunos", "respostas", "vagas")):
            return jsonify({"ok": False, "erro": "Envie os 3 arquivos: alunos, respostas, vagas"}), 400

        df_a = pd.read_csv(io.BytesIO(request.files["alunos"].read()), encoding="utf-8-sig")
        df_r = pd.read_csv(io.BytesIO(request.files["respostas"].read()), encoding="utf-8-sig")
        df_v = pd.read_csv(io.BytesIO(request.files["vagas"].read()), encoding="utf-8-sig")
        
        df_res, saldo_final = processar_alocacao(df_a, df_r, df_v)
        
        # Salva resultado em CSV
        df_res.to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")
        
        # Salva metadados (Analise)
        analise = {
            "rodada_ts": datetime.now().isoformat(),
            "total_candidatos": len(df_res),
            "alocados": len(df_res[df_res["status"] == "ALOCADO"]),
            "nao_alocados": len(df_res[df_res["status"] == "NAO_ALOCADO"]),
            "saldo_remanescente": saldo_final
        }
        with open(META_JSON, "w", encoding="utf-8") as f:
            json.dump(analise, f, ensure_ascii=False, indent=2)

        return jsonify({
            "ok": True,
            "analise": analise,
            "data": df_res.head(50).to_dict(orient="records")
        })

    except Exception as e:
        return jsonify({"ok": False, "erro": str(e), "trace": traceback.format_exc()}), 500

@app.route("/resultado/csv", methods=["GET"])
def baixar_csv():
    if not SAIDA_CSV.exists(): abort(404)
    return send_file(SAIDA_CSV, as_attachment=True, download_name="resultado_lotacao.csv")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
