-- Seeds brasileiros para o crawler
-- Execute no banco após criar as tabelas

INSERT INTO search_engine.crawler_seeds (url, priority, label) VALUES
  -- Tecnologia e desenvolvimento
  ('https://www.tabnews.com.br', 10, 'tecnologia'),
  ('https://dev.to', 9, 'tecnologia'),
  ('https://css-tricks.com', 8, 'frontend'),
  ('https://www.alura.com.br/artigos', 9, 'tecnologia-br'),
  ('https://blog.rocketseat.com.br', 9, 'tecnologia-br'),
  ('https://www.treinaweb.com.br/blog', 8, 'tecnologia-br'),
  ('https://www.devmedia.com.br', 7, 'tecnologia-br'),
  ('https://imasters.com.br', 7, 'tecnologia-br'),
  ('https://tableless.com.br', 7, 'frontend-br'),

  -- Documentações que todo dev usa
  ('https://developer.mozilla.org/pt-BR', 10, 'documentacao'),
  ('https://laravel.com/docs', 10, 'documentacao-laravel'),
  ('https://vuejs.org/guide', 10, 'documentacao-vue'),
  ('https://docs.python.org/pt-br/3', 9, 'documentacao-python'),
  ('https://www.php.net/manual/pt_BR', 9, 'documentacao-php'),

  -- Notícias e conteúdo geral BR
  ('https://www.tecmundo.com.br', 8, 'noticias-tech-br'),
  ('https://olhardigital.com.br', 7, 'noticias-tech-br'),
  ('https://canaltech.com.br', 7, 'noticias-tech-br'),
  ('https://tecnoblog.net', 8, 'noticias-tech-br'),

  -- Ciência e conhecimento
  ('https://pt.wikipedia.org/wiki/Especial:Aleatório', 6, 'wikipedia-br'),
  ('https://www.scielo.br', 7, 'ciencia-br'),
  ('https://www.nexojornal.com.br', 7, 'jornalismo-br'),

  -- Comunidades
  ('https://stackoverflow.com/questions', 9, 'comunidade'),
  ('https://github.com/explore', 8, 'opensource')

ON CONFLICT (url) DO NOTHING;

-- Popula a fila inicial a partir das seeds
INSERT INTO search_engine.crawler_queue (url, domain, priority, depth, status)
SELECT
  url,
  regexp_replace(url, '^https?://([^/]+).*$', '\1') AS domain,
  priority,
  0,
  'pending'
FROM search_engine.crawler_seeds
ON CONFLICT (url) DO NOTHING;