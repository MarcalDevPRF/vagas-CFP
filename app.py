import io
import os
import json
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, send_file, abort
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

app = Flask(__name__)

# Configurações de Diretório
WORKDIR = Path(os.environ.get("LOTACAO_WORKDIR", "/tmp/lotacao"))
WORKDIR.mkdir(parents=True, exist_ok=True)
SAIDA_CSV  = WORKDIR / "resultado_lotacao.csv"
SAIDA_XLSX = WORKDIR / "resultado_lotacao.xlsx"
META_JSON  = WORKDIR / "ultima_rodada.json"

# ─────────────────────────────────────────────────────────────────────────────
# 1. TRATAMENTO DE DADOS (NORMALIZAÇÃO)
# ─────────────────────────────────────────────────────────────────────────────

def _col(df, *nomes):
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in nomes:
        if n.lower() in mapa: return mapa[n.lower()]
    return None

def preparar_df_alunos(df):
    """Normaliza e ordena as sublistas por mérito (Nota e Idade)."""
    df.columns = df.columns.str.strip()
    
    # Mapeamento essencial
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição")
    c_nota = _col(df, "pontuacao", "pontos", "nota")
    c_nasc = _col(df, "data_nascimento", "nascimento")
    c_conc = _col(df, "concorrencia_aluno", "concorrencia")
    c_sit  = _col(df, "situacao_aluno", "situacao")

    df = df.rename(columns={c_insc: "insc", c_nota: "nota", c_nasc: "nasc", c_conc: "conc", c_sit: "sit"})
    
    # Tipagem
    df["nota"] = pd.to_numeric(df["nota"], errors="coerce").fillna(0)
    df["nasc"] = pd.to_datetime(df["nasc"], dayfirst=True, errors="coerce")
    df["conc"] = df["conc"].fillna("AMPLA").get(df["conc"], "AMPLA").astype(str).str.upper()
    df["sit"]  = df["sit"].fillna("REGULAR").astype(str).str.upper()
    
    # Para ordenação de desempate (mais velho primeiro)
    df["nasc_sort"] = df["nasc"].fillna(pd.Timestamp("2099-12-31"))

    # Separar e ordenar cada balde por MÉRITO
    def ordenar(sub_df):
        return sub_df.sort_values(["nota", "nasc_sort"], ascending=[False, True]).reset_index(drop=True)

    listas = {
        "AMPLA": ordenar(df[df["conc"] == "AMPLA"]),
        "COTA_NEGRO": ordenar(df[df["conc"] == "COTA_NEGRO"]),
        "COTA_PCD": ordenar(df[df["conc"] == "COTA_PCD"])
    }
    return listas

# ─────────────────────────────────────────────────────────────────────────────
# 2. MOTOR DE RECLASSIFICAÇÃO (FILA ÚNICA)
# ─────────────────────────────────────────────────────────────────────────────

def gerar_fila_unica(listas_por_concorrencia):
    """
    Cria a classificação final baseada na alternância de cotas e mérito.
    Implementa a VAGA ESPELHO para Subjudices.
    """
    ptr = {"AMPLA": 0, "COTA_NEGRO": 0, "COTA_PCD": 0}
    fila_final = []
    pos_ordinal = 1 # Contador que define a posição na fila

    # Enquanto houver alguém em qualquer lista
    while any(ptr[k] < len(listas_por_concorrencia[k]) for k in ptr):
        
        # Determina qual cota "seria" a vez legal
        tipo_da_vez = "AMPLA"
        if pos_ordinal == 5 or (pos_ordinal > 21 and (pos_ordinal - 21) % 20 == 0):
            tipo_da_vez = "COTA_PCD"
        elif (pos_ordinal - 3) >= 0 and (pos_ordinal - 3) % 5 == 0:
            tipo_da_vez = "COTA_NEGRO"

        # Se a cota da vez acabou, cai para Ampla. Se Ampla acabou, pega o que sobrar.
        if ptr[tipo_da_vez] >= len(listas_por_concorrencia[tipo_da_vez]):
            if ptr["AMPLA"] < len(listas_por_concorrencia["AMPLA"]):
                tipo_da_vez = "AMPLA"
            else:
                tipo_da_vez = next((k for k in ["COTA_NEGRO", "COTA_PCD"] if ptr[k] < len(listas_por_concorrencia[k])), None)

        if not tipo_da_vez: break

        # Pega o próximo candidato por mérito daquela lista
        cand = listas_por_concorrencia[tipo_da_vez].iloc[ptr[tipo_da_vez]].to_dict()
        ptr[tipo_da_vez] += 1
        
        cand["posicao_final"] = pos_ordinal
        fila_final.append(cand)

        # REGRA VAGA ESPELHO: 
        # Se o candidato for SUBJUDICE, ele ganha a posição mas NÃO incrementa o contador global
        # permitindo que o próximo REGULAR ocupe o "mesmo" índice de vaga.
        if cand["sit"] == "REGULAR":
            pos_ordinal += 1

    return pd.DataFrame(fila_final)

# ─────────────────────────────────────────────────────────────────────────────
# 3. PROCESSO DE ALOCAÇÃO (DISTRIBUIÇÃO SERIAL)
# ─────────────────────────────────────────────────────────────────────────────

def processar_alocacao(df_alunos_raw, df_respostas_raw, df_vagas_raw):
    # 1. Preparar Listas por Mérito
    listas = preparar_df_alunos(df_alunos_raw)
    
    # 2. Gerar Classificação Final (Fila Única)
    df_classificado = gerar_fila_unica(listas)
    
    # 3. Carregar Vagas e Respostas
    saldo_vagas = carregar_vagas_dict(df_vagas_raw)
    vagas_orig = dict(saldo_vagas)
    respostas_map = carregar_respostas_map(df_respostas_raw)
    opcao_cols = [c for c in df_respostas_raw.columns if c.lower().startswith("opcao_")]

    resultados = []

    # 4. Distribuir Vagas seguindo a Fila Única
    for _, cand in df_classificado.iterrows():
        insc = str(cand["insc"]).strip()
        resp = respostas_map.get(insc)
        
        res = {
            "posicao_final": cand["posicao_final"],
            "inscricao_aluno": cand["insc"],
            "nome_aluno": cand.get("nome_aluno", "N/A"),
            "situacao_aluno": cand["sit"],
            "concorrencia": cand["conc"],
            "pontuacao": cand["nota"],
            "unidade_alocada": "",
            "status": "NAO_ALOCADO",
            "observacao": ""
        }

        if not resp:
            res["status"] = "SEM_ESCOLHA"
            resultados.append(res)
            continue

        # Lógica de Cônjuge (R3/R4)
        acom_conj = str(resp.get("acom_conjuge", "")).upper() in ("SIM", "S")
        conj_reg  = str(resp.get("situacao_conjuge", "")).upper() == "REGULAR"
        
        opcoes = [str(resp.get(c, "")).strip().upper() for c in opcao_cols if str(resp.get(c, "")).strip()]
        
        alocado = False
        for unid in opcoes:
            vagas_disp = saldo_vagas.get(unid, 0)
            
            # Subjudice não consome vaga do saldo principal (vaga espelho/extra)
            # Mas se for Regular com Cônjuge Regular, consome 2.
            vagas_necessarias = 0
            if cand["sit"] == "REGULAR":
                vagas_necessarias = 2 if (acom_conj and conj_reg) else 1
            
            if vagas_disp >= vagas_necessarias:
                if cand["sit"] == "REGULAR":
                    saldo_vagas[unid] -= vagas_necessarias
                
                res["unidade_alocada"] = unid
                res["status"] = "ALOCADO"
                res["observacao"] = f"Consumiu {vagas_necessarias} vagas" if cand["sit"]=="REGULAR" else "Subjudice (Vaga Espelho)"
                alocado = True
                break
        
        if not alocado and not res["unidade_alocada"]:
            res["status"] = "NAO_ALOCADO"
            res["observacao"] = "Vagas esgotadas nas opções marcadas"
            
        resultados.append(res)

    return pd.DataFrame(resultados), saldo_vagas, vagas_orig

# --- Funções Auxiliares de Carga ---
def carregar_vagas_dict(df):
    unid_col = _col(df, "unidade")
    vaga_col = _col(df, "vagas")
    return dict(zip(df[unid_col].str.upper(), pd.to_numeric(df[vaga_col], errors="coerce").fillna(0)))

def carregar_respostas_map(df):
    # Normaliza inscrição para evitar erro de .0
    c_insc = _col(df, "inscricao_aluno", "inscricao", "inscrição", "Inscrição_aluno")
    df["insc_norm"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    return df.set_index("insc_norm").to_dict(orient="index")

# ─────────────────────────────────────────────────────────────────────────────
# 4. ENDPOINTS E EXPORTAÇÃO (REDUZIDO PARA FOCO NA LÓGICA)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/classificar")
def classificar():
    try:
        # Recebimento dos arquivos
        df_alunos = pd.read_csv(request.files["alunos"], encoding="utf-8-sig")
        df_resp   = pd.read_csv(request.files["respostas"], encoding="utf-8-sig")
        df_vagas  = pd.read_csv(request.files["vagas"], encoding="utf-8-sig")
        
        df_final, saldo, orig = processar_alocacao(df_alunos, df_resp, df_vagas)
        
        # Salva para downloads posteriores
        df_final.to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")
        
        return jsonify({
            "ok": True,
            "mensagem": "Lotação processada com sucesso seguindo a Fila Única por Mérito.",
            "estatisticas": {
                "total": len(df_final),
                "alocados": len(df_final[df_final["status"] == "ALOCADO"])
            },
            "preview": df_final.head(20).to_dict(orient="records")
        })
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
