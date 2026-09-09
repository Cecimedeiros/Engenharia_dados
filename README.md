# Engenharia_dados

## 🎯 Objetivo

Este repositório foi criado para prática de atividades relacionadas à engenharia de dados.

---

## ⚙️ Configuração do Ambiente

```bash
python -m venv venv         # Criando ambiente virtual na sua máquina
venv\Scripts\Activate.ps1   # Ativando o ambiente virtual

pip install requests black
```

---

## 🧪 Testes de API (Postman & IBGE)

Como parte das práticas do projeto, foram estruturados testes automatizados no Postman para validar os endpoints públicos do **Serviço de Dados do IBGE**.

### 📌 Endpoints Testados

#### 1. Agregados IBGE (`GET`)

- **Endpoint:** `/api/v3/agregados/4093/periodos/201201-202602/variaveis/4096`
- **Métrica:** Taxa de participação na força de trabalho.

**Validações de Teste:**

- Validação de Status Code (`200 OK`)
- Validação do formato de resposta em JSON
- Checagem do ID da variável (`4096`) e tipo `string`
- Confirmação do nome da variável contendo o termo correto

#### 2. Regiões (`GET`)

- **Endpoint:** `/api/v1/localidades/regioes`
- **Métrica:** Lista das grandes regiões do Brasil.

**Validações de Teste:**

- Validação de Status Code (`200 OK`)
- Validação do formato de resposta em JSON
- Confirmação do retorno de exatamente 5 regiões
- Verificação das propriedades obrigatórias (`id`, `sigla`, `nome`) em cada região

---

## ⚙️ Códigos dos Scripts de Teste (Postman)

### 1. Testes para o endpoint `Agregados IBGE`

```javascript
pm.test("Status code deve ser 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Resposta deve ser JSON", function () {
    pm.response.to.be.json;
});

pm.test("ID da variável deve ser 4096", function () {
    const resposta = pm.response.json();
    pm.expect(resposta[0].id).to.eql("4096");
});

pm.test("Variável deve ser taxa de participação na força de trabalho", function () {
    const resposta = pm.response.json();
    pm.expect(resposta[0].variavel)
        .to.include("Taxa de participação na força de trabalho");
});

pm.test("O ID deve ser um texto", function () {
    const resposta = pm.response.json();
    pm.expect(resposta[0].id).to.be.a("string");
});
```

### 2. Testes para o endpoint `Regiões`

```javascript
pm.test("Status code é 200 OK", function () {
    pm.response.to.have.status(200);
});

pm.test("A resposta deve ser JSON", function () {
    pm.response.to.be.json;
});

pm.test("Retorna exatamente 5 regiões", function () {
    const resposta = pm.response.json();
    pm.expect(resposta.length).to.eql(5);
});

pm.test("Cada região possui id, sigla e nome", function () {
    const resposta = pm.response.json();

    resposta.forEach(function(regiao) {
        pm.expect(regiao).to.have.property('id');
        pm.expect(regiao).to.have.property('sigla');
        pm.expect(regiao).to.have.property('nome');
    });
});
```

---

## 🚀 Como Executar a Coleção de Testes

1. Faça o download do arquivo `Engenharia de Dados - API IBGE.postman_collection.json` presente na raiz deste repositório.
2. Abra o **Postman** e clique em **Import**.
3. Escolha o arquivo `.json` baixado.
4. Defina a variável `baseUrl` como:

```text
https://servicodados.ibge.gov.br
```

5. Clique em **Send** em cada requisição ou utilize o **Collection Runner** para executar toda a suíte de testes.

---

## 👥 Integrantes

- Arthur Estevão
- Alvaro Silva
- Beatriz Paredes
- Cecília Medeiros
- Icaro Silva
- Isabella Batista
- Jose Leandro De Morais
- Melissa Filgueiras
- Gabriel Souza
- Thays Barbosa