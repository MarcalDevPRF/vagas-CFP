"""
SERVIÇO DE CLASSIFICAÇÃO - PRF
================================
Recebe CSV de respostas (respostas_atual) + CSV de alunos (com classificacao),
aplica a regra de alternância AMPLA / COTA_NEGRO / PCD e devolve:
  - JSON com a lista classificada (para exibição na tela)
  - PDF para download

Endpoints:
  POST /classificar   — body: multipart/form-data com campos:
                          respostas (arquivo CSV)
                          alunos    (arquivo CSV)
  GET  /health        — verifica se o serviço está online
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import io
import json
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
import traceback

app = Flask(__name__)
CORS(app)  # Permite chamadas do Apps Script

# ─── Constantes de alternância ────────────────────────────────────────────────
# Posições (1-based) reservadas para cada cota na lista final geral
# Negro: posições 3, 8, 13, 18, 23, 28, 33, 38 ... (a cada 5, começando na 3)
# PcD:   posições 5, 21, 41, 61 ...
# Demais posições: AMPLA

def posicao_tipo(pos):
    """
    Dado um número de posição (1-based), retorna qual tipo ocupa aquela vaga.
    Lógica:
      - PcD:   posições 5, 21, 41, 61, 81 ... (posição 5 e depois a cada 20)
      - Negro: posições 3, 8, 13, 18, 23 ... (a cada 5, começando na 3,
               exceto quando já ocupada pelo PcD)
      - AMPLA: todo o resto
    """
    # PcD: posição 5 e depois 21, 41, 61 ... (5 + 16, depois a cada 20)
    if pos == 5:
        return 'PCD'
    if pos > 5 and (pos - 21) >= 0 and (pos - 21) % 20 == 0:
        return 'PCD'
    # Negro: a cada 5 a partir da posição 3 (3,8,13,18,23...)
    if pos >= 3 and (pos - 3) % 5 == 0:
        return 'COTA_NEGRO'
    return 'AMPLA'


def classificar(df_resp, df_alunos):
    """
    Executa a classificação completa.

    Parâmetros
    ----------
    df_resp   : DataFrame da aba respostas_atual
                colunas esperadas: inscricao (ou matricula), pontuacao, concorrencia,
                                   nome, papel, timestamp, opcao_1..opcao_N
    df_alunos : DataFrame da aba Alunos com coluna 'classificacao'
                colunas esperadas: inscricao_aluno, classificacao, concorrencia_aluno

    Retorna
    -------
    lista : list of dict com a classificação final
    avisos: list of str com alertas
    """
    avisos = []

    # ── Normaliza nomes de colunas ──────────────────────────────────────────
    df_resp.columns   = [c.strip().lower() for c in df_resp.columns]
    df_alunos.columns = [c.strip().lower() for c in df_alunos.columns]

    # Aceita 'matricula' ou 'inscricao' como chave
    if 'matricula' in df_resp.columns and 'inscricao' not in df_resp.columns:
        df_resp = df_resp.rename(columns={'matricula': 'inscricao'})
    if 'inscricao_aluno' in df_alunos.columns:
        df_alunos = df_alunos.rename(columns={'inscricao_aluno': 'inscricao'})
    if 'concorrencia_aluno' in df_alunos.columns:
        df_alunos = df_alunos.rename(columns={'concorrencia_aluno': 'concorrencia'})

    # Converte chaves para string sem espaços
    df_resp['inscricao']   = df_resp['inscricao'].astype(str).str.strip()
    df_alunos['inscricao'] = df_alunos['inscricao'].astype(str).str.strip()

    # ── Filtra apenas candidatos (exclui acompanhantes) ────────────────────
    if 'papel' in df_resp.columns:
        df_cand = df_resp[df_resp['papel'].str.lower() != 'acompanhante'].copy()
    else:
        df_cand = df_resp.copy()

    if df_cand.empty:
        return [], ['Nenhum candidato encontrado nas respostas.']

    # ── Cruza classificação vinda da aba Alunos ─────────────────────────────
    cols_alunos = ['inscricao', 'classificacao']
    if 'concorrencia' in df_alunos.columns:
        cols_alunos.append('concorrencia')
    df_merge = df_cand.merge(
        df_alunos[cols_alunos],
        on='inscricao', how='left', suffixes=('', '_aluno')
    )

    # Usa concorrência da aba alunos se disponível; caso contrário da resposta
    if 'concorrencia_aluno' in df_merge.columns:
        df_merge['concorrencia'] = df_merge['concorrencia_aluno'].combine_first(
            df_merge.get('concorrencia', pd.Series(dtype=str))
        )

    # Normaliza concorrência para maiúsculas sem espaços extras
    df_merge['concorrencia'] = df_merge['concorrencia'].astype(str).str.strip().str.upper()

    # Converte classificação para numérico
    df_merge['classificacao'] = pd.to_numeric(df_merge['classificacao'], errors='coerce')

    sem_class = df_merge['classificacao'].isna().sum()
    if sem_class > 0:
        avisos.append(f'{sem_class} candidato(s) sem classificação definida — serão listados ao final.')

    # ── Monta as 3 listas por concorrência ordenadas pela classificação ─────
    tipos = ['AMPLA', 'COTA_NEGRO', 'PCD']
    listas = {}
    for t in tipos:
        sub = df_merge[df_merge['concorrencia'] == t].copy()
        sub = sub.sort_values('classificacao', ascending=True, na_position='last')
        listas[t] = sub.reset_index(drop=True)

    # ── Aplica alternância para montar lista final ──────────────────────────
    # Ponteiros para cada lista
    ptrs = {t: 0 for t in tipos}
    resultado = []
    pos = 1

    # Quantas posições no máximo (soma de todos os candidatos)
    total = sum(len(listas[t]) for t in tipos)

    while len(resultado) < total:
        tipo_pos = posicao_tipo(pos)

        # Verifica se a lista do tipo reservado tem candidatos
        if ptrs[tipo_pos] < len(listas[tipo_pos]):
            tipo_escolhido = tipo_pos
        else:
            # Lista reservada vazia — chama AMPLA
            if ptrs['AMPLA'] < len(listas['AMPLA']):
                tipo_escolhido = 'AMPLA'
                if tipo_pos != 'AMPLA':
                    avisos.append(
                        f'Posição {pos} reservada para {tipo_pos} '
                        f'preenchida por AMPLA (lista {tipo_pos} esgotada).'
                    )
            else:
                # AMPLA também esgotada — pega qualquer lista restante
                restantes = [t for t in tipos if ptrs[t] < len(listas[t])]
                if not restantes:
                    break
                tipo_escolhido = restantes[0]

        row = listas[tipo_escolhido].iloc[ptrs[tipo_escolhido]].to_dict()
        ptrs[tipo_escolhido] += 1

        # Descobre as opções escolhidas
        opcoes = []
        for i in range(1, 50):
            col = f'opcao_{i}'
            if col in row and str(row[col]).strip():
                opcoes.append(str(row[col]).strip())
            else:
                break

        resultado.append({
            'posicao_geral':  pos,
            'tipo_vaga':      tipo_escolhido,
            'classificacao':  int(row.get('classificacao', 0)) if not pd.isna(row.get('classificacao', float('nan'))) else '—',
            'inscricao':      str(row.get('inscricao', '')),
            'nome':           str(row.get('nome', '')),
            'concorrencia':   str(row.get('concorrencia', '')),
            'pontuacao':      row.get('pontuacao', ''),
            'opcoes':         opcoes,
        })
        pos += 1

    return resultado, avisos


def gerar_pdf(resultado, avisos, certame):
    """Gera PDF com a lista de classificação e retorna bytes."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=2*cm, bottomMargin=1.5*cm
    )

    styles = getSampleStyleSheet()
    titulo_style = ParagraphStyle(
        'titulo', parent=styles['Heading1'],
        fontSize=14, alignment=TA_CENTER, textColor=colors.HexColor('#002c53'),
        spaceAfter=6
    )
    sub_style = ParagraphStyle(
        'sub', parent=styles['Normal'],
        fontSize=9, alignment=TA_CENTER, textColor=colors.grey,
        spaceAfter=12
    )
    aviso_style = ParagraphStyle(
        'aviso', parent=styles['Normal'],
        fontSize=8, textColor=colors.HexColor('#92400e'), spaceAfter=4
    )

    elems = []
    elems.append(Paragraph(f'LISTA DE CLASSIFICAÇÃO — {certame.upper()}', titulo_style))
    elems.append(Paragraph(f'Total de candidatos classificados: {len(resultado)}', sub_style))

    if avisos:
        for av in avisos:
            elems.append(Paragraph(f'⚠ {av}', aviso_style))
        elems.append(Spacer(1, 0.3*cm))

    # Cabeçalho da tabela
    header = ['Pos.', 'Tipo Vaga', 'Class.', 'Inscrição', 'Nome', 'Concorrência', 'Pontuação', '1ª Opção', '2ª Opção', '3ª Opção']
    data = [header]

    cor_tipo = {
        'AMPLA':      colors.HexColor('#e8f4fd'),
        'COTA_NEGRO': colors.HexColor('#fdf3e7'),
        'PCD':        colors.HexColor('#edf7ed'),
    }

    for r in resultado:
        opcoes = r['opcoes']
        data.append([
            str(r['posicao_geral']),
            r['tipo_vaga'],
            str(r['classificacao']),
            r['inscricao'],
            r['nome'][:35] + ('…' if len(r['nome']) > 35 else ''),
            r['concorrencia'],
            str(r['pontuacao']),
            opcoes[0] if len(opcoes) > 0 else '',
            opcoes[1] if len(opcoes) > 1 else '',
            opcoes[2] if len(opcoes) > 2 else '',
        ])

    col_widths = [1.2*cm, 2.8*cm, 1.5*cm, 2.5*cm, 7*cm, 3.2*cm, 2.5*cm, 4*cm, 4*cm, 4*cm]
    t = Table(data, colWidths=col_widths, repeatRows=1)

    style = TableStyle([
        ('BACKGROUND',   (0, 0), (-1, 0), colors.HexColor('#002c53')),
        ('TEXTCOLOR',    (0, 0), (-1, 0), colors.white),
        ('FONTNAME',     (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE',     (0, 0), (-1, 0), 8),
        ('ALIGN',        (0, 0), (-1, -1), 'CENTER'),
        ('ALIGN',        (4, 1), (4, -1), 'LEFT'),
        ('ALIGN',        (7, 1), (-1, -1), 'LEFT'),
        ('FONTSIZE',     (0, 1), (-1, -1), 7.5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f7f9fc')]),
        ('GRID',         (0, 0), (-1, -1), 0.25, colors.HexColor('#d1d9e6')),
        ('VALIGN',       (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING',   (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING',(0, 0), (-1, -1), 3),
    ])

    # Colorir linhas por tipo de vaga
    for i, r in enumerate(resultado, start=1):
        bg = cor_tipo.get(r['tipo_vaga'], colors.white)
        style.add('BACKGROUND', (1, i), (1, i), bg)

    t.setStyle(style)
    elems.append(t)
    doc.build(elems)
    buf.seek(0)
    return buf


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'})


@app.route('/classificar', methods=['POST'])
def endpoint_classificar():
    try:
        # Valida arquivos enviados
        if 'respostas' not in request.files:
            return jsonify({'erro': 'Arquivo respostas não enviado.'}), 400
        if 'alunos' not in request.files:
            return jsonify({'erro': 'Arquivo alunos não enviado.'}), 400

        certame = request.form.get('certame', 'Certame')
        formato = request.args.get('formato', 'json')  # json ou pdf

        # Lê os CSVs
        f_resp   = request.files['respostas']
        f_alunos = request.files['alunos']

        df_resp   = pd.read_csv(io.StringIO(f_resp.read().decode('utf-8-sig')))
        df_alunos = pd.read_csv(io.StringIO(f_alunos.read().decode('utf-8-sig')))

        # Executa classificação
        resultado, avisos = classificar(df_resp, df_alunos)

        if formato == 'pdf':
            pdf_buf = gerar_pdf(resultado, avisos, certame)
            return send_file(
                pdf_buf,
                mimetype='application/pdf',
                as_attachment=True,
                download_name=f'classificacao_{certame}.pdf'
            )

        # Retorna JSON
        return jsonify({
            'certame':   certame,
            'total':     len(resultado),
            'avisos':    avisos,
            'resultado': resultado
        })

    except Exception as e:
        return jsonify({'erro': str(e), 'trace': traceback.format_exc()}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
