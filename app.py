"""
app.py  ─  API de Lotação CFP  (Render)
========================================
Endpoints
---------
GET  /health                       → liveness probe do Render
POST /processar                    → recebe os 3 CSVs, roda a alocação
                                     e devolve JSON com resultado + resumo
GET  /resultado/csv                → baixa resultado_lotacao.csv (última rodada)
GET  /resultado/xlsx               → baixa resultado_lotacao.xlsx (última rodada)
GET  /saldo                        → saldo final de vagas da última rodada
GET  /analise                      → análise estatística da última rodada

Uso rápido (curl):
    curl -X POST https://<app>.onrender.com/processar \
         -F "classificacao=@lista_ordenada_cfp.csv" \
         -F "vagas=@vagas.csv" \
         -F "escolhas=@escolhas.csv"
"""

import io
import os
import json
import tempfile
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd
from flask import Flask, jsonify, request, send_file, abort
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024   # 50 MB

# Diretório de trabalho (persistente dentro do container)
WORKDIR = Path(os.environ.get("LOTACAO_WORKDIR", "/tmp/lotacao"))
WORKDIR.mkdir(parents=True, exist_ok=True)

SAIDA_CSV  = WORKDIR / "resultado_lotacao.csv"
SAIDA_XLSX = WORKDIR / "resultado_lotacao.xlsx"
META_JSON  = WORKDIR / "ultima_rodada.json"


# ═════════════════════════════════════════════════════════════════════════════
# LÓGICA DE ALOCAÇÃO  (idêntica ao alocar_lotacao.py original)
# ═════════════════════════════════════════════════════════════════════════════

def carregar_dados(arq_classif: Path, arq_vagas: Path, arq_escolhas: Path):
    # Classificação
    classif = pd.read_csv(arq_classif)
    classif["inscricao_aluno"] = classif["inscricao_aluno"].astype(str).str.strip()

    # Vagas
    vagas_df = pd.read_csv(arq_vagas)
    vagas_df.columns = vagas_df.columns.str.strip()
    vagas_df["nome_unidade"] = vagas_df["nome_unidade"].str.strip().str.upper()
    vagas_df["vagas"] = vagas_df["vagas"].astype(int)
    saldo_vagas = dict(zip(vagas_df["nome_unidade"], vagas_df["vagas"]))
    vagas_originais = dict(saldo_vagas)   # snapshot para relatório

    # Escolhas
    escolhas = pd.read_csv(arq_escolhas)
    escolhas.columns = escolhas.columns.str.strip()
    escolhas["Inscrição"] = escolhas["Inscrição"].astype(str).str.strip()

    opcao_cols = sorted(
        [c for c in escolhas.columns if c.startswith("opcao_")],
        key=lambda x: int(x.split("_")[1])
    )
    for col in opcao_cols:
        escolhas[col] = (escolhas[col].astype(str).str.strip().str.upper()
                         .replace({"NAN": "", "NONE": ""}))

    escolhas["acom_conjuge"]      = escolhas["acom_conjuge"].astype(str).str.strip().str.upper()
    escolhas["situacao_conjuge"]  = escolhas["situacao_conjuge"].astype(str).str.strip().str.upper()
    escolhas["matricula_conjuge"] = escolhas["matricula_conjuge"].astype(str).str.strip()

    return classif, saldo_vagas, vagas_originais, escolhas, opcao_cols


def get_opcoes(row_escolha, opcao_cols):
    return [row_escolha[col] for col in opcao_cols if row_escolha[col]]


def _resultado(posicao, inscricao, nome, situacao,
               status, unidade, vagas_consumidas=0, obs=""):
    return {
        "posicao_final":    posicao,
        "inscricao_aluno":  inscricao,
        "nome_aluno":       nome,
        "situacao_aluno":   situacao,
        "unidade_alocada":  unidade or "",
        "status":           status,
        "vagas_consumidas": vagas_consumidas,
        "observacao":       obs,
    }


def alocar_candidato(cand, escolha, saldo_vagas, opcao_cols):
    inscricao    = str(cand["inscricao_aluno"])
    nome         = cand["nome_aluno"]
    situacao     = cand["situacao_aluno"]
    posicao      = cand["posicao_final"]
    consome_vaga = (situacao == "REGULAR")           # R5

    if escolha is None:
        return _resultado(posicao, inscricao, nome, situacao,
                          status="SEM_ESCOLHA", unidade=None,
                          obs="Candidato não encontrado no CSV de escolhas")

    acom_conjuge    = escolha["acom_conjuge"] == "SIM"
    sit_conjuge     = escolha.get("situacao_conjuge", "")
    conjuge_regular = sit_conjuge == "REGULAR"

    opcoes = get_opcoes(escolha, opcao_cols)
    if not opcoes:
        return _resultado(posicao, inscricao, nome, situacao,
                          status="SEM_OPCAO", unidade=None,
                          obs="Nenhuma opção preenchida")

    for unidade in opcoes:
        saldo_atual = saldo_vagas.get(unidade, 0)
        vagas_necessarias = 1
        if acom_conjuge and consome_vaga:            # R3 / R4
            vagas_necessarias = 2 if conjuge_regular else 1

        if saldo_atual >= vagas_necessarias:
            if consome_vaga:                         # R2
                saldo_vagas[unidade] -= vagas_necessarias

            obs_parts = []
            if not consome_vaga:
                obs_parts.append("SUBJUDICE: não consumiu vaga")
            if acom_conjuge:
                obs_parts.append(
                    "cônjuge REGULAR alocado junto (−2 vagas)"
                    if conjuge_regular else "cônjuge SUBJUDICE (−1 vaga)"
                )
            return _resultado(posicao, inscricao, nome, situacao,
                              status="ALOCADO", unidade=unidade,
                              vagas_consumidas=vagas_necessarias if consome_vaga else 0,
                              obs="; ".join(obs_parts))

    return _resultado(posicao, inscricao, nome, situacao,   # R8
                      status="NAO_ALOCADO", unidade=None,
                      obs=f"Sem vaga nas {len(opcoes)} opções informadas")


def processar_alocacao(arq_classif, arq_vagas, arq_escolhas):
    classif, saldo_vagas, vagas_orig, escolhas, opcao_cols = carregar_dados(
        arq_classif, arq_vagas, arq_escolhas
    )
    idx_escolhas = escolhas.set_index("Inscrição")
    resultados = []

    for _, cand in classif.iterrows():               # R1
        inscricao = str(cand["inscricao_aluno"])
        escolha = idx_escolhas.loc[inscricao] if inscricao in idx_escolhas.index else None
        if isinstance(escolha, pd.DataFrame):
            escolha = escolha.sort_values("timestamp").iloc[-1]

        resultado = alocar_candidato(cand, escolha, saldo_vagas, opcao_cols)
        resultado["concorrencia_aluno"] = cand["concorrencia_aluno"]
        resultado["pontuacao"]          = cand["pontuacao"]
        resultado["motivo_chamada"]     = cand.get("motivo_chamada", "")
        resultados.append(resultado)

    df = pd.DataFrame(resultados)[[
        "posicao_final", "inscricao_aluno", "nome_aluno",
        "concorrencia_aluno", "situacao_aluno", "pontuacao",
        "motivo_chamada", "unidade_alocada", "status",
        "vagas_consumidas", "observacao",
    ]]
    return df, saldo_vagas, vagas_orig


# ═════════════════════════════════════════════════════════════════════════════
# EXPORTAÇÃO XLSX
# ═════════════════════════════════════════════════════════════════════════════

CORES_STATUS = {
    "ALOCADO":     "D4EDDA",
    "NAO_ALOCADO": "F8D7DA",
    "SEM_ESCOLHA": "FFF3CD",
    "SEM_OPCAO":   "FFE0B2",
}
CORES_SUBJ = {"ALOCADO": "B8DFC4", "NAO_ALOCADO": "F1AFBB"}
LARGURAS = {
    "posicao_final": 10, "inscricao_aluno": 14, "nome_aluno": 38,
    "concorrencia_aluno": 16, "situacao_aluno": 14, "pontuacao": 10,
    "motivo_chamada": 18, "unidade_alocada": 30, "status": 16,
    "vagas_consumidas": 16, "observacao": 45,
}


def exportar_xlsx(df: pd.DataFrame, caminho: Path):
    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Lotação")
        ws = writer.sheets["Lotação"]

        fill_hdr  = PatternFill("solid", fgColor="1F3864")
        font_hdr  = Font(bold=True, color="FFFFFF", size=11)
        borda     = Border(
            left=Side(style="thin", color="CCCCCC"),
            right=Side(style="thin", color="CCCCCC"),
            bottom=Side(style="thin", color="CCCCCC"),
        )
        for cell in ws[1]:
            cell.font = font_hdr
            cell.fill = fill_hdr
            cell.alignment = Alignment(horizontal="center", vertical="center")

        status_idx = list(df.columns).index("status")
        sit_idx    = list(df.columns).index("situacao_aluno")

        for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
            status = row[status_idx].value or ""
            sit    = row[sit_idx].value    or ""
            cor    = (CORES_SUBJ.get(status, "FFFFFF")
                      if sit == "SUBJUDICE"
                      else CORES_STATUS.get(status, "FFFFFF"))
            fill = PatternFill("solid", fgColor=cor)
            for cell in row:
                cell.fill = fill
                cell.border = borda
                cell.alignment = Alignment(vertical="center")

        for i, col in enumerate(df.columns, 1):
            ws.column_dimensions[get_column_letter(i)].width = LARGURAS.get(col, 14)
        ws.freeze_panes = "A2"


# ═════════════════════════════════════════════════════════════════════════════
# ANÁLISE ESTATÍSTICA
# ═════════════════════════════════════════════════════════════════════════════

def gerar_analise(df: pd.DataFrame, saldo_final: dict, vagas_orig: dict) -> dict:
    total    = len(df)
    alocados = df[df["status"] == "ALOCADO"]

    # ── Distribuição por status ──────────────────────────────────────────────
    dist_status = df["status"].value_counts().to_dict()

    # ── Distribuição por situação (REGULAR / SUBJUDICE) ──────────────────────
    dist_sit = df["situacao_aluno"].value_counts().to_dict()

    # ── Distribuição por concorrência ────────────────────────────────────────
    dist_conc = df["concorrencia_aluno"].value_counts().to_dict()

    # ── Ocupação por unidade ─────────────────────────────────────────────────
    ocup_unidade = []
    for unidade, vagas in sorted(vagas_orig.items()):
        consumidas = vagas - saldo_final.get(unidade, 0)
        ocup_unidade.append({
            "unidade":    unidade,
            "vagas_total":     vagas,
            "vagas_ocupadas":  consumidas,
            "vagas_restantes": saldo_final.get(unidade, 0),
            "ocupacao_pct":    round(consumidas / vagas * 100, 1) if vagas else 0,
        })

    # ── Preferência de unidade (1ª opção mais escolhida) ─────────────────────
    # Calculamos a partir dos alocados: unidade mais popular
    top_unidades = (
        alocados["unidade_alocada"]
        .value_counts()
        .head(10)
        .rename_axis("unidade")
        .reset_index(name="alocados")
        .to_dict(orient="records")
    )

    # ── Taxa de sucesso na 1ª opção ───────────────────────────────────────────
    # Não temos a preferência salva no df final, mas calculamos proxy:
    # candidatos alocados / total com escolha
    com_escolha = total - dist_status.get("SEM_ESCOLHA", 0) - dist_status.get("SEM_OPCAO", 0)
    taxa_alocacao = round(len(alocados) / com_escolha * 100, 1) if com_escolha else 0

    # ── Pontuação média por status ────────────────────────────────────────────
    pont_media = (
        df.groupby("status")["pontuacao"]
        .mean()
        .round(2)
        .to_dict()
    )

    return {
        "resumo": {
            "total_candidatos":  total,
            "alocados":          len(alocados),
            "nao_alocados":      dist_status.get("NAO_ALOCADO", 0),
            "sem_escolha":       dist_status.get("SEM_ESCOLHA", 0),
            "sem_opcao":         dist_status.get("SEM_OPCAO", 0),
            "taxa_alocacao_pct": taxa_alocacao,
        },
        "por_status":          dist_status,
        "por_situacao":        dist_sit,
        "por_concorrencia":    dist_conc,
        "ocupacao_unidades":   ocup_unidade,
        "top10_unidades_alocadas": top_unidades,
        "pontuacao_media_por_status": pont_media,
    }


# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    """Liveness probe exigido pelo Render."""
    return jsonify({"status": "ok", "ts": datetime.utcnow().isoformat()})


@app.post("/processar")
def processar():
    """
    Recebe os 3 CSVs via multipart/form-data:
      - classificacao  → lista_ordenada_cfp.csv
      - vagas          → vagas.csv
      - escolhas       → escolhas.csv
    Retorna JSON com resumo + primeiros 50 alocados.
    Salva CSV/XLSX no WORKDIR para download posterior.
    """
    erros = []
    for campo in ("classificacao", "vagas", "escolhas"):
        if campo not in request.files:
            erros.append(f"Campo '{campo}' ausente no multipart.")
    if erros:
        return jsonify({"ok": False, "erros": erros}), 400

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        arqs = {}
        for campo, nome in [("classificacao", "lista_ordenada_cfp.csv"),
                             ("vagas",          "vagas.csv"),
                             ("escolhas",       "escolhas.csv")]:
            destino = tmp / nome
            request.files[campo].save(destino)
            arqs[campo] = destino

        try:
            df, saldo_final, vagas_orig = processar_alocacao(
                arqs["classificacao"], arqs["vagas"], arqs["escolhas"]
            )
        except Exception as exc:
            return jsonify({"ok": False, "erro": str(exc),
                            "trace": traceback.format_exc()}), 500

    # Salvar resultados
    df.to_csv(SAIDA_CSV, index=False, encoding="utf-8-sig")
    exportar_xlsx(df, SAIDA_XLSX)

    analise = gerar_analise(df, saldo_final, vagas_orig)

    # Salvar metadados da última rodada
    META_JSON.write_text(json.dumps({
        "rodada_ts": datetime.utcnow().isoformat(),
        "analise":   analise,
        "saldo_final": saldo_final,
    }, ensure_ascii=False, indent=2))

    # Montar resposta: resumo + primeiros 50 alocados
    alocados_head = (df[df["status"] == "ALOCADO"]
                     .head(50)
                     .to_dict(orient="records"))

    return jsonify({
        "ok":             True,
        "rodada_ts":      datetime.utcnow().isoformat(),
        "analise":        analise,
        "primeiros_50_alocados": alocados_head,
        "download_csv":  "/resultado/csv",
        "download_xlsx": "/resultado/xlsx",
    })


@app.get("/resultado/csv")
def resultado_csv():
    if not SAIDA_CSV.exists():
        abort(404, "Nenhuma rodada processada ainda. Use POST /processar.")
    return send_file(SAIDA_CSV, as_attachment=True,
                     download_name="resultado_lotacao.csv",
                     mimetype="text/csv")


@app.get("/resultado/xlsx")
def resultado_xlsx():
    if not SAIDA_XLSX.exists():
        abort(404, "Nenhuma rodada processada ainda. Use POST /processar.")
    return send_file(SAIDA_XLSX, as_attachment=True,
                     download_name="resultado_lotacao.xlsx",
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/saldo")
def saldo():
    """Saldo final de vagas da última rodada."""
    if not META_JSON.exists():
        abort(404, "Nenhuma rodada processada ainda.")
    meta = json.loads(META_JSON.read_text())
    return jsonify({
        "ok": True,
        "rodada_ts":   meta["rodada_ts"],
        "saldo_final": meta["saldo_final"],
    })


@app.get("/analise")
def analise():
    """Análise estatística completa da última rodada."""
    if not META_JSON.exists():
        abort(404, "Nenhuma rodada processada ainda.")
    meta = json.loads(META_JSON.read_text())
    return jsonify({
        "ok": True,
        "rodada_ts": meta["rodada_ts"],
        **meta["analise"],
    })


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
