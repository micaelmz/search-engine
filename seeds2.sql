-- Seeds Genéricos e Multitemáticos (Brasil)
INSERT INTO search_engine.crawler_seeds (url, priority, label) VALUES
  -- Grandes Portais (Notícias, Entretenimento, Economia)
  ('https://www.uol.com.br', 10, 'geral-br'),
  ('https://g1.globo.com', 10, 'noticias-br'),
  ('https://www.r7.com', 9, 'noticias-br'),
  ('https://www.folha.uol.com.br', 9, 'jornalismo-br'),
  ('https://www.estadao.com.br', 9, 'jornalismo-br'),
  ('https://www.metropoles.com', 8, 'geral-br'),

  -- Governo e Utilidade Pública (Autoridade Máxima de Domínio)
  ('https://www.gov.br/pt-br', 10, 'governo-br'),
  ('https://www.ibge.gov.br', 9, 'dados-br'),
  ('https://www.bcb.gov.br', 8, 'financas-br'),
  ('https://www.saude.gov.br', 8, 'saude-br'),

  -- Educação e Academia (Excelentes para achar referências e artigos)
  ('https://www.usp.br', 9, 'educacao-br'),
  ('https://www.unicamp.br', 8, 'educacao-br'),
  ('https://www.capes.gov.br', 8, 'ciencia-br'),
  ('https://brasilescola.uol.com.br', 7, 'educacao-br'),
  ('https://www.todamateria.com.br', 7, 'educacao-br'),

  -- Lifestyle, Esportes e Cultura
  ('https://ge.globo.com', 9, 'esportes-br'),
  ('https://www.tudogostoso.com.br', 8, 'gastronomia-br'),
  ('https://www.guiadasemana.com.br', 7, 'cultura-br'),
  ('https://catracalivre.com.br', 7, 'eventos-br'),
  ('https://www.omelete.com.br', 8, 'entretenimento-br'),

  -- Hubs de Negócios e Consumo (Ótimos para descobrir novos sites/marcas)
  ('https://www.reclameaqui.com.br', 9, 'consumo-br'),
  ('https://www.infomoney.com.br', 8, 'negocios-br'),
  ('https://www.ecommercebrasil.com.br', 7, 'ecommerce-br')

ON CONFLICT (url) DO NOTHING;