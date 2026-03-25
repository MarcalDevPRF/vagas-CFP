"""
app.py — Render Service: classificação e alocação de lotação CFP
================================================================
Endpoint principal: POST /classificar
Recebe multipart/form-data com:
  - certame   : string com o nome do certame
  - alunos    : CSV da aba Alunos_<certame>
  - respostas : CSV da aba respostas_atual_<certame>
  - vagas     : CSV da aba vagas_<certame>

Retorna JSON:
  { ok, total, resultado: [...], avisos: [...] }

onde cada item de resultado tem:
  posicao_geral, tipo_vaga, classificacao, inscricao, nome,
  concorrencia, situacao, pontuacao, opcoes[], unidade_alocada, obs
"""

import os
import traceback
from datetime import date
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, request, send_file

app   = Flask(__name__)
WDIR  = Path("/tmp/lotacao")
WDIR.mkdir(parents=True, exist_ok=True)
SAIDA = WDIR / "resultado_lotacao.csv"

# ── Helpers de coluna ─────────────────────────────────────────────────────────

def _col(df, *candidatos):
    """Retorna o nome real da primeira coluna que casar (case-insensitive)."""
    mapa = {c.lower().strip(): c for c in df.columns}
    for n in candidatos:
        if n.lower() in mapa:
            return mapa[n.lower()]
    return None


def _norm_str(v):
    """Normaliza strings: strip + upper, trata NaN."""
    if pd.isna(v):
        return ""
    return str(v).strip().upper()


# ── 1. Carregar vagas ─────────────────────────────────────────────────────────

def carregar_vagas(df_v):
    """
    Retorna dict { NOME_UNIDADE_UPPER: saldo_int }.
    Aceita colunas 'unidade' ou 'nome_unidade'; 'vagas' ou 'quantidade'.
    """
    c_unid = _col(df_v, "unidade", "nome_unidade")
    c_qtd  = _col(df_v, "vagas", "quantidade")
    if not c_unid or not c_qtd:
        raise ValueError("CSV de vagas precisa das colunas 'unidade' e 'vagas'.")
    saldo = {}
    for _, row in df_v.iterrows():
        u = _norm_str(row[c_unid])
        q = int(pd.to_numeric(row[c_qtd], errors="coerce") or 0)
        if u:
            saldo[u] = q
    return saldo


# ── 2. Carregar respostas ─────────────────────────────────────────────────────

def carregar_respostas(df_r):
    """
    Retorna dict { inscricao_str: row_dict }.
    Detecta dinamicamente quantas colunas opcao_N existem.
    Mantém apenas a linha mais recente por inscrição (keep='last').
    """
    c_insc = _col(df_r, "inscricao_aluno", "inscricao")
    if not c_insc:
        raise ValueError("CSV de respostas precisa da coluna 'inscricao_aluno'.")

    # Normalizar inscrição (remove .0 de int→float→str)
    df_r = df_r.copy()
    df_r["_insc"] = (df_r[c_insc].astype(str)
                     .str.replace(r"\.0$", "", regex=True)
                     .str.strip())

    # Detectar colunas de opção dinamicamente
    opcao_cols = sorted(
        [c for c in df_r.columns if c.lower().startswith("opcao_")],
        key=lambda x: int(x.lower().split("_")[1]) if x.lower().split("_")[1].isdigit() else 0
    )

    # Normalizar colunas de opção para UPPER
    for col in opcao_cols:
        df_r[col] = df_r[col].astype(str).str.strip().str.upper().replace({"NAN": "", "NONE": ""})

    # Normalizar acom_conjuge (trata NÃO com e sem acento, S/N abreviados)
    if _col(df_r, "acom_conjuge"):
        c_acom = _col(df_r, "acom_conjuge")
        df_r[c_acom] = (df_r[c_acom].astype(str).str.strip().str.upper()
                        .str.normalize("NFKD")
                        .str.encode("ascii", errors="ignore")
                        .str.decode("ascii"))
        # Agora está sem acento: "SIM", "NAO", "S", "N"

    # Manter última resposta por inscrição
    df_r = df_r.drop_duplicates(subset=["_insc"], keep="last")
    return df_r.set_index("_insc").to_dict(orient="index"), opcao_cols


# ── 3. Montar fila classificada ───────────────────────────────────────────────

def _calcular_idade(nasc):
    """Retorna idade em anos a partir de um Timestamp."""
    if pd.isna(nasc):
        return 0
    hoje = date.today()
    return hoje.year - nasc.year - ((hoje.month, hoje.day) < (nasc.month, nasc.day))


def _montar_fila_concorrencia(df_alunos, concorrencia):
    """
    Para uma dada concorrência, separa REGULAR e SUBJUDICE,
    ordena cada grupo por (nota↓, idade↓) e intercala os SUBJUDICE
    logo após o REGULAR de nota ≤ (espelhamento).

    REGRA R6: SUBJUDICE é processado ANTES do REGULAR empatado.
    """
    c_insc = _col(df_alunos, "inscricao_aluno", "inscricao")
    c_nome = _col(df_alunos, "nome_aluno", "nome")
    c_nota = _col(df_alunos, "pontuacao", "nota")
    c_nasc = _col(df_alunos, "data_nascimento")
    c_conc = _col(df_alunos, "concorrencia_aluno", "concorrencia")
    c_sit  = _col(df_alunos, "situacao_aluno", "situacao")

    sub = df_alunos[
        df_alunos[c_conc].astype(str).str.upper().str.strip() == concorrencia
    ].copy()

    if sub.empty:
        return []

    # Calcular idade
    if c_nasc:
        sub["_nasc"] = pd.to_datetime(sub[c_nasc], errors="coerce")
        sub["_idade"] = sub["_nasc"].apply(_calcular_idade)
    else:
        sub["_idade"] = 0

    sub["_nota"] = pd.to_numeric(sub[c_nota], errors="coerce").fillna(0)
    sub["_sit"]  = sub[c_sit].astype(str).str.upper().str.strip()
    sub["_insc"] = sub[c_insc].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    sub["_nome"] = sub[c_nome].astype(str)

    # Ordenar por nota↓, idade↓ (maior idade = prioridade = data de nascimento menor)
    regular   = (sub[sub["_sit"] == "REGULAR"]
                 .sort_values(["_nota", "_idade"], ascending=[False, False])
                 .reset_index(drop=True))
    subjudice = (sub[sub["_sit"] == "SUBJUDICE"]
                 .sort_values(["_nota", "_idade"], ascending=[False, False])
                 .reset_index(drop=True))

    # Espelhamento: inserir SUBJUDICE logo após o REGULAR de nota imediatamente ≤
    fila = []
    pendentes = subjudice.to_dict("records")

    for _, reg_row in regular.iterrows():
        fila.append({
            "insc":  reg_row["_insc"],
            "nome":  reg_row["_nome"],
            "nota":  reg_row["_nota"],
            "idade": reg_row["_idade"],
            "conc":  concorrencia,
            "sit":   "REGULAR",
        })
        # Inserir SUBJUDICE cuja nota >= nota deste REGULAR (espelho)
        ainda = []
        for s in pendentes:
            if s["_nota"] >= reg_row["_nota"]:
                fila.append({
                    "insc":  s["_insc"],
                    "nome":  s["_nome"],
                    "nota":  s["_nota"],
                    "idade": s["_idade"],
                    "conc":  concorrencia,
                    "sit":   "SUBJUDICE",
                })
            else:
                ainda.append(s)
        pendentes = ainda

    # SUBJUDICE sem espelho → final da fila
    for s in pendentes:
        fila.append({
            "insc":  s["_insc"],
            "nome":  s["_nome"],
            "nota":  s["_nota"],
            "idade": s["_idade"],
            "conc":  concorrencia,
            "sit":   "SUBJUDICE",
        })

    return fila


def montar_fila_global(df_alunos):
    """
    Aplica o relógio de cotas sobre as filas internas de cada concorrência.

    RELÓGIO:
      - COTA (20%): posições 3, 8, 13, 18 … → (pos − 3) % 5 == 0
      - PCD   (5%): posições 5, 25, 45, 65 … → (pos − 5) % 20 == 0
      - Demais: AMPLA

    REGRA DE FALLBACK: se a fila de cota estiver vazia, a posição
    reservada é preenchida por AMPLA; o relógio avança normalmente.

    Os valores 'COTA' e 'PCD' vêm da planilha — mapeamos para
    os nomes legíveis que o frontend exibe.
    """
    # Mapear nomes da planilha → nome interno
    # A planilha usa: AMPLA, COTA, PCD
    fila_ampla = _montar_fila_concorrencia(df_alunos, "AMPLA")
    fila_cota  = _montar_fila_concorrencia(df_alunos, "COTA")
    fila_pcd   = _montar_fila_concorrencia(df_alunos, "PCD")

    ptrs  = {"AMPLA": 0, "COTA": 0, "PCD": 0}
    filas = {"AMPLA": fila_ampla, "COTA": fila_cota, "PCD": fila_pcd}
    total = len(fila_ampla) + len(fila_cota) + len(fila_pcd)

    resultado = []

    for pos in range(1, total + 1):
        # Determinar tipo da vaga pelo relógio
        # REGRA: PCD tem precedência sobre COTA se ambos coincidirem
        if pos >= 5 and (pos - 5) % 20 == 0:
            tipo_vez = "PCD"
        elif pos >= 3 and (pos - 3) % 5 == 0:
            tipo_vez = "COTA"
        else:
            tipo_vez = "AMPLA"

        # Fallback se fila do tipo estiver esgotada
        if ptrs[tipo_vez] >= len(filas[tipo_vez]):
            # Tenta AMPLA → COTA → PCD na ordem
            for fallback in ["AMPLA", "COTA", "PCD"]:
                if ptrs[fallback] < len(filas[fallback]):
                    tipo_vez = fallback
                    break
            else:
                break  # todas as filas esgotadas

        cand = filas[tipo_vez][ptrs[tipo_vez]].copy()
        cand["posicao_final"] = pos
        cand["tipo_vaga"]     = tipo_vez
        resultado.append(cand)
        ptrs[tipo_vez] += 1

    return resultado


# ── 4. Alocar candidatos ──────────────────────────────────────────────────────

def alocar(fila, resp_map, opcao_cols, saldo_vagas):
    """
    Percorre a fila na ordem de classificação e aloca cada candidato.

    REGRAS:
      R1  Ordem de processamento = posicao_final (já garantida pela fila).
      R2  Cada alocação reduz o saldo da unidade.
      R3  Candidato com acom_conjuge=SIM só pode ser alocado onde saldo >= vagas_necessarias.
      R4  Cônjuge REGULAR consome +1 vaga (total 2); cônjuge SUBJUDICE não consome vaga extra.
      R5  SUBJUDICE não consome vaga (vaga espelho).
      R6  Dentro de cada posição, SUBJUDICE vem antes do REGULAR (garantido pelo espelhamento).
      R7  Candidato percorre opções em ordem; aloca na primeira viável.
      R8  Sem opção viável → NAO_ALOCADO.
    """
    resultados   = []
    class_count  = {"AMPLA": 0, "COTA": 0, "PCD": 0}
    avisos       = []

    for cand in fila:
        insc  = str(cand["insc"])
        conc  = cand["conc"]
        sit   = cand["sit"]
        is_sub = (sit == "SUBJUDICE")

        class_count[conc] = class_count.get(conc, 0) + 1

        resp = resp_map.get(insc, {})

        # Opções do candidato em ordem de preferência
        opcoes = []
        for col in opcao_cols:
            v = _norm_str(resp.get(col, ""))
            if v:
                opcoes.append(v)

        # Dados de cônjuge
        acom_raw   = _norm_str(resp.get("acom_conjuge", ""))
        quer_conj  = acom_raw in ("SIM", "S")
        sit_conj   = _norm_str(resp.get("situacao_conjuge", ""))
        conj_reg   = (sit_conj == "REGULAR")

        # R5: SUBJUDICE não consome vaga
        # R4: cônjuge REGULAR consome +1; cônjuge SUBJUDICE não
        if is_sub:
            custo = 0
        elif quer_conj and conj_reg:
            custo = 2   # titular + cônjuge REGULAR
        else:
            custo = 1   # apenas o titular

        unidade_alocada = "NAO_ALOCADO"
        obs             = ""
        alocado         = False

        if not opcoes:
            obs = "Sem escolhas registradas"
        else:
            for u in opcoes:
                saldo_atual = saldo_vagas.get(u, 0)

                # R3: verificar saldo suficiente
                # Para SUBJUDICE (custo=0): não precisa de vaga disponível
                # mas ainda verifica se a unidade existe
                if u not in saldo_vagas:
                    continue

                if is_sub or saldo_atual >= custo:
                    # R2: reduzir saldo (apenas para REGULAR)
                    if not is_sub:
                        saldo_vagas[u] -= custo

                    unidade_alocada = u
                    if is_sub:
                        obs = "Vaga espelho (SUBJUDICE, não consome saldo)"
                    elif custo == 2:
                        obs = "Alocado com cônjuge REGULAR (−2 vagas)"
                    else:
                        obs = "Alocado (−1 vaga)"
                    alocado = True
                    break

            if not alocado:
                obs = f"Vagas esgotadas em todas as {len(opcoes)} opções"
                avisos.append(f"{cand['nome']} ({insc}): não alocado — {obs}")

        resultados.append({
            "posicao_geral":   int(cand["posicao_final"]),
            "tipo_vaga":       conc,
            "classificacao":   class_count[conc],
            "inscricao":       insc,
            "nome":            cand["nome"],
            "concorrencia":    conc,
            "situacao":        sit,
            "pontuacao":       float(cand["nota"]) if pd.notna(cand["nota"]) else 0.0,
            "opcoes":          opcoes,
            "unidade_alocada": unidade_alocada,
            "obs":             obs,
        })

    return resultados, avisos


# ── 5. Rotas Flask ────────────────────────────────────────────────────────────

@app.route("/health", methods=["GET"])
def health():
    """Endpoint de verificação de saúde — usado pelo Admin para testar conexão."""
    return jsonify({"ok": True, "status": "online"})


@app.route("/classificar", methods=["POST"])
def classificar():
    try:
        # Validar arquivos obrigatórios
        for campo in ("alunos", "respostas", "vagas"):
            if campo not in request.files:
                return jsonify({
                    "ok": False,
                    "erro": f"Campo obrigatório ausente no multipart: '{campo}'"
                }), 400

        # Ler CSVs — utf-8-sig para remover BOM gerado pelo Sheets
        df_a = pd.read_csv(request.files["alunos"],   encoding="utf-8-sig")
        df_r = pd.read_csv(request.files["respostas"], encoding="utf-8-sig")
        df_v = pd.read_csv(request.files["vagas"],    encoding="utf-8-sig")

        certame = request.form.get("certame", "")

        # Processar
        saldo_vagas          = carregar_vagas(df_v)
        resp_map, opcao_cols = carregar_respostas(df_r)
        fila                 = montar_fila_global(df_a)
        resultados, avisos   = alocar(fila, resp_map, opcao_cols, saldo_vagas)

        # Salvar CSV de resultado
        pd.DataFrame([{
            "Classificação":    r["posicao_geral"],
            "Tipo":             r["tipo_vaga"],
            "Class. no Grupo":  r["classificacao"],
            "Inscrição":        r["inscricao"],
            "Nome":             r["nome"],
            "Concorrência":     r["concorrencia"],
            "Situação":         r["situacao"],
            "Pontuação":        r["pontuacao"],
            "1ª Opção":         r["opcoes"][0] if r["opcoes"] else "",
            "2ª Opção":         r["opcoes"][1] if len(r["opcoes"]) > 1 else "",
            "Unidade Alocada":  r["unidade_alocada"],
            "Obs":              r["obs"],
        } for r in resultados]).to_csv(SAIDA, index=False, encoding="utf-8-sig")

        return jsonify({
            "ok":        True,
            "certame":   certame,
            "total":     len(resultados),
            "resultado": resultados,
            "avisos":    avisos,
        })

    except Exception:
        tb = traceback.format_exc()
        app.logger.error(tb)
        return jsonify({"ok": False, "erro": tb}), 500


@app.route("/resultado/csv", methods=["GET"])
def download_csv():
    """Download do último CSV de resultado gerado."""
    if not SAIDA.exists():
        return jsonify({"ok": False, "erro": "Nenhum resultado disponível."}), 404
    return send_file(SAIDA, as_attachment=True, download_name="resultado_lotacao.csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
