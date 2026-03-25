import io
import os
import traceback
import pandas as pd
from pathlib import Path
from flask import Flask, jsonify, request, send_file

app = Flask(__name__)
WORKDIR = Path("/tmp/lotacao")
WORKDIR.mkdir(parents=True, exist_ok=True)
SAIDA_CSV = WORKDIR / "resultado_lotacao.csv"

def _col(df, *nomes):
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in nomes:
        if n.lower() in mapa: return mapa[n.lower()]
    return None

def carregar_vagas_dict(df):
    unid_col = _col(df, "unidade", "nome_unidade")
    vaga_col = _col(df, "vagas", "quantidade")
    return dict(zip(df[unid_col].astype(str).str.upper().str.strip(), 
                    pd.to_numeric(df[vaga_col], errors="coerce").fillna(0).astype(int)))

def carregar_respostas_map(df):
    c_insc = _col(df, "inscricao_aluno", "inscricao")
    df["insc_norm"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    return df.drop_duplicates(subset=["insc_norm"], keep="last").set_index("insc_norm").to_dict(orient="index")

def preparar_listas(df):
    # Padronização de colunas
    c_insc = _col(df, "inscricao_aluno", "inscricao")
    c_nota = _col(df, "pontuacao", "nota")
    c_nasc = _col(df, "data_nascimento")
    c_conc = _col(df, "concorrencia_aluno")
    c_sit  = _col(df, "situacao_aluno")

    df_p = pd.DataFrame()
    df_p["insc"] = df[c_insc].astype(str).str.replace(".0", "", regex=False).str.strip()
    df_p["nome"] = df[_col(df, "nome_aluno", "nome")].astype(str)
    df_p["nota"] = pd.to_numeric(df[c_nota], errors="coerce").fillna(0)
    df_p["nasc"] = pd.to_datetime(df[c_nasc], errors="coerce")
    df_p["conc"] = df[c_conc].astype(str).str.upper().fillna("AMPLA")
    df_p["sit"]  = df[c_sit].astype(str).str.upper().fillna("REGULAR")
    df_p["nasc_sort"] = df_p["nasc"].fillna(pd.Timestamp("2099-12-31"))
    
    def ordenar(sub): return sub.sort_values(["nota", "nasc_sort"], ascending=[False, True]).reset_index(drop=True)

    return {
        "AMPLA": ordenar(df_p[df_p["conc"] == "AMPLA"]),
        "COTA":  ordenar(df_p[df_p["conc"] == "COTA"]),
        "PCD":   ordenar(df_p[df_p["conc"] == "PCD"])
    }

def gerar_fila_com_prioridade_subjudice(listas):
    ptr = {"AMPLA": 0, "COTA": 0, "PCD": 0}
    fila_final = []
    pos_vaga_regular = 1
    
    while any(ptr[k] < len(listas[k]) for k in ptr):
        tipo_vez = "AMPLA"
        if pos_vaga_regular == 5 or (pos_vaga_regular > 21 and (pos_vaga_regular - 21) % 20 == 0):
            tipo_vez = "PCD"
        elif (pos_vaga_regular >= 3) and ((pos_vaga_regular - 3) % 5 == 0):
            tipo_vez = "COTA"

        if ptr[tipo_vez] >= len(listas[tipo_vez]):
            tipo_vez = "AMPLA" if ptr["AMPLA"] < len(listas["AMPLA"]) else next((k for k in ptr if ptr[k] < len(listas[k])), None)
        
        if not tipo_vez: break
        
        # --- LÓGICA DE PRIORIDADE SUBJUDICE NA MESMA POSIÇÃO ---
        # Pegamos todos os candidatos que "empatariam" na mesma vaga (Subjudices + 1 Regular)
        candidatos_da_posicao = []
        
        while ptr[tipo_vez] < len(listas[tipo_vez]):
            cand_atual = listas[tipo_vez].iloc[ptr[tipo_vez]].to_dict()
            candidatos_da_posicao.append(cand_atual)
            ptr[tipo_vez] += 1
            # Se pegamos um REGULAR, paramos o grupo desta posição
            if cand_atual["sit"] == "REGULAR":
                break
        
        # Ordenamos o grupo: SUBJUDICE primeiro, REGULAR por último
        # No Python, True (Subjudice) vem depois de False, então invertemos ou usamos key
        candidatos_da_posicao.sort(key=lambda x: 0 if x["sit"] == "SUBJUDICE" else 1)
        
        for c in candidatos_da_posicao:
            c["posicao_final"] = pos_vaga_regular
            fila_final.append(c)
        
        pos_vaga_regular += 1
            
    return pd.DataFrame(fila_final)

@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        df_a = pd.read_csv(request.files["alunos"], encoding="utf-8-sig")
        df_r = pd.read_csv(request.files["respostas"], encoding="utf-8-sig")
        df_v = pd.read_csv(request.files["vagas"], encoding="utf-8-sig")
        
        listas = preparar_listas(df_a)
        df_fila = gerar_fila_com_prioridade_subjudice(listas)
        vagas = carregar_vagas_dict(df_v)
        resp_map = carregar_respostas_map(df_r)
        
        resultados = []
        opcao_cols = [f"opcao_{i}" for i in range(1, 29)]
        class_count = {"AMPLA": 0, "COTA": 0, "PCD": 0}

        for _, c in df_fila.iterrows():
            r = resp_map.get(str(c["insc"]), {})
            conc = str(c["conc"])
            class_count[conc] = class_count.get(conc, 0) + 1
            tipo_vaga = "COTA_NEGRO" if conc == "COTA" else conc
            opcoes = [str(r.get(opt, "")).strip().upper() for opt in opcao_cols if r.get(opt)]

            res = {
                # Campos JSON para o frontend
                "posicao_geral":  int(c["posicao_final"]),
                "tipo_vaga":      tipo_vaga,
                "classificacao":  class_count[conc],
                "inscricao":      c["insc"],
                "nome":           c["nome"],
                "concorrencia":   conc,
                "situacao":       c["sit"],
                "pontuacao":      float(c["nota"]) if pd.notna(c["nota"]) else 0.0,
                "opcoes":         opcoes,
                "unidade_alocada": "NÃO ALOCADO",
                "obs":            "",
            }

            alocado = False
            for u in opcoes:
                if u in vagas:
                    is_sub = (c["sit"] == "SUBJUDICE")
                    quer_conj = str(r.get("acom_conjuge", "")).upper() in ("SIM","S")
                    conj_reg = str(r.get("situacao_conjuge", "")).upper() == "REGULAR"

                    custo = 2 if (quer_conj and conj_reg and not is_sub) else (1 if not is_sub else 0)

                    # Se for Subjudice, ele SEMPRE consegue espelhar se a unidade existe no mapa de vagas
                    # Se for Regular, precisa ter saldo >= custo
                    if is_sub or vagas[u] >= custo:
                        if not is_sub:
                            vagas[u] -= custo

                        res["unidade_alocada"] = u
                        res["obs"] = "Alocado (Vaga Espelho)" if is_sub else f"Alocado (Custo: {custo})"
                        alocado = True
                        break

            if not alocado:
                res["obs"] = "Vagas esgotadas" if opcoes else "Sem escolhas registradas"
            resultados.append(res)

        # Salva CSV com nomes de colunas em português (compatibilidade legada)
        csv_rows = [{
            "Classificação":  r["posicao_geral"],
            "Inscrição":      r["inscricao"],
            "Nome":           r["nome"],
            "Cota":           r["concorrencia"],
            "Situação":       r["situacao"],
            "Unidade Alocada": r["unidade_alocada"],
            "Obs":            r["obs"],
        } for r in resultados]
        pd.DataFrame(csv_rows).to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")

        return jsonify({"ok": True, "total": len(resultados), "resultado": resultados, "avisos": []})
    except Exception:
        return jsonify({"ok": False, "trace": traceback.format_exc()}), 500

@app.route("/resultado/csv")
def dload(): return send_file(SAIDA_CSV, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
