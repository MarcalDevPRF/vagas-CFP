// ─── SUBSTITUIR as funções abaixo no Admin.json ──────────────────────────────

/**
 * Exporta os 3 CSVs: respostas_atual, Alunos e vagas do certame ativo.
 * [ATUALIZADO] Inclui vagas.csv para o Render processar a alocação.
 */
function adminExportarCsvExecucao() {
  var auth = _assertAdmin_();
  if (!auth.ok) return { sucesso: false, message: auth.message };
  try {
    var cfg = _getCfg_();
    if (!cfg.concursoAtivo) return { sucesso: false, message: "Nenhum certame ativo." };
    var ss = SpreadsheetApp.getActiveSpreadsheet();

    // CSV de respostas_atual (última escolha de cada candidato)
    var shResp = ss.getSheetByName(cfg.respostasAtualSheetName);
    if (!shResp || shResp.getLastRow() < 2)
      return { sucesso: false, message: "Nenhuma resposta encontrada para o certame ativo." };
    var csvResp = _sheetToCsv_(shResp);

    // CSV de alunos (com classificação/pontuação)
    var shAlunos = ss.getSheetByName(cfg.alunosSheetName);
    if (!shAlunos || shAlunos.getLastRow() < 2)
      return { sucesso: false, message: "Nenhum aluno encontrado para o certame ativo." };
    var csvAlunos = _sheetToCsv_(shAlunos);

    // CSV de vagas (unidade + qtd)
    var shVagas = ss.getSheetByName(cfg.vagasSheetName);
    if (!shVagas || shVagas.getLastRow() < 2)
      return { sucesso: false, message: "Nenhuma vaga encontrada para o certame ativo." };
    var csvVagas = _sheetToCsv_(shVagas);

    return {
      sucesso: true,
      certame: cfg.concursoAtivo,
      csvRespostas: csvResp,
      csvAlunos: csvAlunos,
      csvVagas: csvVagas,
      totalRespostas: shResp.getLastRow() - 1,
      totalAlunos: shAlunos.getLastRow() - 1,
      totalVagas: shVagas.getLastRow() - 1
    };
  } catch(e) { return { sucesso: false, message: "Erro: " + e.message }; }
}

/**
 * Envia os 3 CSVs ao Render e retorna o JSON de alocação.
 * [ATUALIZADO] Endpoint /classificar, inclui vagas.csv.
 */
function adminChamarRender(renderUrl, csvRespostas, csvAlunos, certame, csvVagas) {
  var auth = _assertAdmin_();
  if (!auth.ok) return { sucesso: false, message: auth.message };
  try {
    var url = renderUrl.replace(/\/$/, '') + '/classificar';
    var boundary = 'boundary_' + Utilities.getUuid().replace(/-/g, '');
    var crlf = '\r\n';

    var bodyParts = [];
    var addField = function(name, value) {
      bodyParts.push(
        '--' + boundary + crlf +
        'Content-Disposition: form-data; name="' + name + '"' + crlf + crlf +
        value
      );
    };
    var addFile = function(name, filename, content) {
      bodyParts.push(
        '--' + boundary + crlf +
        'Content-Disposition: form-data; name="' + name + '"; filename="' + filename + '"' + crlf +
        'Content-Type: text/csv' + crlf + crlf +
        content
      );
    };

    addField('certame', certame);
    addFile('respostas', 'respostas.csv', csvRespostas);
    addFile('alunos',    'alunos.csv',    csvAlunos);
    addFile('vagas',     'vagas.csv',     csvVagas);   // ← NOVO

    var body = bodyParts.join(crlf) + crlf + '--' + boundary + '--';

    var resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'multipart/form-data; boundary=' + boundary,
      payload: Utilities.newBlob(body).getBytes(),
      muteHttpExceptions: true
    });

    var code = resp.getResponseCode();
    var text = resp.getContentText();
    if (code !== 200)
      return { sucesso: false, message: 'Render retornou HTTP ' + code + ': ' + text.slice(0, 300) };

    var dados = JSON.parse(text);
    if (dados.erro) return { sucesso: false, message: dados.erro };
    return { sucesso: true, dados: dados };
  } catch(e) { return { sucesso: false, message: 'Erro ao chamar Render: ' + e.message }; }
}

/**
 * Solicita o PDF ao Render com os 3 CSVs.
 * [ATUALIZADO] Endpoint /classificar?formato=pdf, inclui vagas.csv.
 */
function adminChamarRenderPdf(renderUrl, certame) {
  var auth = _assertAdmin_();
  if (!auth.ok) return { sucesso: false, message: auth.message };
  try {
    var exp = adminExportarCsvExecucao();
    if (!exp.sucesso) return exp;

    var url = renderUrl.replace(/\/$/, '') + '/classificar?formato=pdf';
    var boundary = 'boundary_' + Utilities.getUuid().replace(/-/g, '');
    var crlf = '\r\n';

    var bodyParts = [];
    var addField = function(name, value) {
      bodyParts.push('--' + boundary + crlf +
        'Content-Disposition: form-data; name="' + name + '"' + crlf + crlf + value);
    };
    var addFile = function(name, filename, content) {
      bodyParts.push('--' + boundary + crlf +
        'Content-Disposition: form-data; name="' + name + '"; filename="' + filename + '"' + crlf +
        'Content-Type: text/csv' + crlf + crlf + content);
    };

    addField('certame', certame || exp.certame);
    addFile('respostas', 'respostas.csv', exp.csvRespostas);
    addFile('alunos',    'alunos.csv',    exp.csvAlunos);
    addFile('vagas',     'vagas.csv',     exp.csvVagas);  // ← NOVO

    var body = bodyParts.join(crlf) + crlf + '--' + boundary + '--';

    var resp = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'multipart/form-data; boundary=' + boundary,
      payload: Utilities.newBlob(body).getBytes(),
      muteHttpExceptions: true
    });

    var code = resp.getResponseCode();
    if (code !== 200) return { sucesso: false, message: 'Render retornou HTTP ' + code };

    var pdfBase64 = Utilities.base64Encode(resp.getContent());
    return { sucesso: true, pdfBase64: pdfBase64 };
  } catch(e) { return { sucesso: false, message: 'Erro ao gerar PDF: ' + e.message }; }
}
