const { Client } = require('pg')
const OpenCC = require('opencc-js')
const { v4: uuidv4 } = require('uuid')

// 中文转换
const converter = OpenCC.Converter({ from: 'tw', to: 'cn' })
const convertTW2CN = (s) =>
  s === '乾隆' ? s : typeof s === 'string' ? converter(s) : s

// 数据库配置
const client = new Client({
  host: 'localhost',
  port: 5432,
  user: 'postgres',
  password: '123456',
  database: 'digua_poetry',
})

async function createTables() {
  // 创建 author 表
  await client.query(`
    CREATE TABLE IF NOT EXISTS author (
      id UUID PRIMARY KEY,
      name VARCHAR(255) UNIQUE NOT NULL,
      name_tw VARCHAR(255),
      dynasty VARCHAR(50),
      introduction TEXT,
      introduction_tw TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `)

  // 创建 poem_tang 表
  await client.query(`
    CREATE TABLE IF NOT EXISTS poem_tang (
      id UUID PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_tw VARCHAR(255),
      content TEXT NOT NULL,
      content_tw TEXT,
      author_name VARCHAR(255),
      author_name_tw VARCHAR(255),
      author_id UUID REFERENCES author(id),
      classify VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `)

  // 创建 poem_song 表
  await client.query(`
    CREATE TABLE IF NOT EXISTS poem_song (
      id UUID PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_tw VARCHAR(255),
      content TEXT NOT NULL,
      content_tw TEXT,
      author_name VARCHAR(255),
      author_name_tw VARCHAR(255),
      author_id UUID REFERENCES author(id),
      classify VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `)

  // 创建 ci_song 表
  await client.query(`
    CREATE TABLE IF NOT EXISTS ci_song (
      id UUID PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_tw VARCHAR(255),
      content TEXT NOT NULL,
      content_tw TEXT,
      author_name VARCHAR(255),
      author_name_tw VARCHAR(255),
      author_id UUID REFERENCES author(id),
      classify VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `)

  // 创建触发器函数
  await client.query(`
    CREATE OR REPLACE FUNCTION update_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
      NEW.updated_at = CURRENT_TIMESTAMP;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `)

  // 创建触发器
  const tables = ['author', 'poem_tang', 'poem_song', 'ci_song']
  for (const table of tables) {
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_${table}_updated_at'
        ) THEN
          CREATE TRIGGER trigger_${table}_updated_at
          BEFORE UPDATE ON ${table}
          FOR EACH ROW
          EXECUTE FUNCTION update_updated_at();
        END IF;
      END$$;
    `)
  }
}

// 导入唐诗宋诗作者
async function importTangSongPoemAuthors() {
  const authorsTang = require('chinese-poetry/chinese-poetry/json/authors.tang.json')
  const authorsSong = require('chinese-poetry/chinese-poetry/json/authors.song.json')

  // 合并作者数据
  const allAuthors = [
    ...authorsTang.map((a) => ({ ...a, dynasty: '唐' })),
    ...authorsSong.map((a) => ({ ...a, dynasty: '宋' })),
  ]

  // 导入作者数据
  for (const item of allAuthors) {
    const id = uuidv4()
    const name = convertTW2CN(item.name)
    const introduction = convertTW2CN(item.desc || '')

    await client.query(
      `INSERT INTO author (id, name, name_tw, dynasty, introduction, introduction_tw)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (name) DO UPDATE SET
         name_tw = EXCLUDED.name_tw,
         dynasty = EXCLUDED.dynasty,
         introduction = EXCLUDED.introduction,
         introduction_tw = EXCLUDED.introduction_tw;`,
      [id, name, item.name, item.dynasty, introduction, item.desc || '']
    )
  }
}

// 导入宋词作者
async function importSongCiAuthors() {
  const authors = require('chinese-poetry/chinese-poetry/ci/author.song.json')

  // 合并作者数据
  const allAuthors = [...authors.map((a) => ({ ...a, dynasty: '宋' }))]

  // 导入作者数据
  for (const item of allAuthors) {
    const id = uuidv4()

    await client.query(
      `INSERT INTO author (id, name, name_tw, dynasty, introduction, introduction_tw)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (name) DO NOTHING;`,
      [id, item.name, null, item.dynasty, item.description, null]
    )
  }
}

// 导入唐诗
async function insertTangPoems() {
  for (let i = 0; i < 58; i++) {
    const poems = require(`chinese-poetry/chinese-poetry/json/poet.tang.${
      i * 1000
    }.json`)
    const values = []

    for (const poem of poems) {
      const authorName = poem.author
      const res = await client.query(
        'SELECT id FROM author WHERE name_tw = $1 LIMIT 1',
        [authorName]
      )
      // if (res.rows.length === 0) {
      //   console.warn(`未找到作者：${authorName}，跳过`)
      //   continue
      // }
      const authorId = res.rows.length > 0 ? res.rows[0].id : null

      values.push({
        id: uuidv4(),
        title: convertTW2CN(poem.title),
        title_tw: poem.title,
        content: Array.isArray(poem.paragraphs)
          ? poem.paragraphs.map(convertTW2CN).join('\n')
          : convertTW2CN(poem.paragraphs),
        content_tw: Array.isArray(poem.paragraphs)
          ? poem.paragraphs.join('\n')
          : poem.paragraphs,
        author_name: convertTW2CN(poem.author),
        author_name_tw: poem.author,
        author_id: authorId,
        classify: '诗',
      })
    }

    if (values.length > 0) {
      const queryText = `
  INSERT INTO poem_tang (id, title, title_tw, content, content_tw, author_name, author_name_tw, author_id, classify)
  VALUES ${values
    .map(
      (_, idx) =>
        `($${idx * 9 + 1}, $${idx * 9 + 2}, $${idx * 9 + 3}, $${
          idx * 9 + 4
        }, $${idx * 9 + 5}, $${idx * 9 + 6}, $${idx * 9 + 7}, $${
          idx * 9 + 8
        }, $${idx * 9 + 9})`
    )
    .join(',')}
`

      const queryValues = values.flatMap((v) => [
        v.id,
        v.title,
        v.title_tw,
        v.content,
        v.content_tw,
        v.author_name,
        v.author_name_tw,
        v.author_id,
        v.classify,
      ])

      try {
        await client.query('BEGIN')
        await client.query(queryText, queryValues)
        await client.query('COMMIT')
        console.log(`成功导入 ${i * 1000} ~ ${(i + 1) * 1000} 条`)
      } catch (e) {
        await client.query('ROLLBACK')
        console.error(`导入第 ${i} 段失败：`, e.message)
      }
    }
  }
}

// 导入宋诗
async function insertSongPoems() {
  for (let i = 0; i < 255; i++) {
    const poems = require(`chinese-poetry/chinese-poetry/json/poet.song.${
      i * 1000
    }.json`)
    const values = []

    for (const poem of poems) {
      const authorName = poem.author
      const res = await client.query(
        'SELECT id FROM author WHERE name_tw = $1 LIMIT 1',
        [authorName]
      )
      const authorId = res.rows.length > 0 ? res.rows[0].id : null

      values.push({
        id: uuidv4(),
        title: convertTW2CN(poem.title),
        title_tw: poem.title,
        content: Array.isArray(poem.paragraphs)
          ? poem.paragraphs.map(convertTW2CN).join('\n')
          : convertTW2CN(poem.paragraphs),
        content_tw: Array.isArray(poem.paragraphs)
          ? poem.paragraphs.join('\n')
          : poem.paragraphs,
        author_name: convertTW2CN(poem.author),
        author_name_tw: poem.author,
        author_id: authorId,
        classify: '诗',
      })
    }

    if (values.length > 0) {
      const queryText = `
  INSERT INTO poem_song (id, title, title_tw, content, content_tw, author_name, author_name_tw, author_id, classify)
  VALUES ${values
    .map(
      (_, idx) =>
        `($${idx * 9 + 1}, $${idx * 9 + 2}, $${idx * 9 + 3}, $${
          idx * 9 + 4
        }, $${idx * 9 + 5}, $${idx * 9 + 6}, $${idx * 9 + 7}, $${
          idx * 9 + 8
        }, $${idx * 9 + 9})`
    )
    .join(',')}
`

      const queryValues = values.flatMap((v) => [
        v.id,
        v.title,
        v.title_tw,
        v.content,
        v.content_tw,
        v.author_name,
        v.author_name_tw,
        v.author_id,
        v.classify,
      ])

      try {
        await client.query('BEGIN')
        await client.query(queryText, queryValues)
        await client.query('COMMIT')
        console.log(`成功导入 ${i * 1000} ~ ${(i + 1) * 1000} 条`)
      } catch (e) {
        await client.query('ROLLBACK')
        console.error(`导入第 ${i} 段失败：`, e.message)
      }
    }
  }
}

async function insertSongCi() {
  // 导入宋词
  for (let i = 0; i < 22; i++) {
    const songCis = require(`chinese-poetry/chinese-poetry/ci/ci.song.${
      i * 1000
    }.json`)
    const values = []
    for (const poem of songCis) {
      const authorName = poem.author
      const res = await client.query(
        'SELECT id FROM author WHERE name = $1 LIMIT 1',
        [authorName]
      )
      const authorId = res.rows.length > 0 ? res.rows[0].id : null

      values.push({
        id: uuidv4(),
        title: poem.rhythmic,
        title_tw: null,
        content: Array.isArray(poem.paragraphs)
          ? poem.paragraphs.join('\n')
          : poem.paragraphs,
        content_tw: null,
        author_name: poem.author,
        author_name_tw: null,
        author_id: authorId,
        classify: '词',
      })
    }

    if (values.length > 0) {
      const queryText = `
  INSERT INTO ci_song (id, title, title_tw, content, content_tw, author_name, author_name_tw, author_id, classify)
  VALUES ${values
    .map(
      (_, idx) =>
        `($${idx * 9 + 1}, $${idx * 9 + 2}, $${idx * 9 + 3}, $${
          idx * 9 + 4
        }, $${idx * 9 + 5}, $${idx * 9 + 6}, $${idx * 9 + 7}, $${
          idx * 9 + 8
        }, $${idx * 9 + 9})`
    )
    .join(',')}
`

      const queryValues = values.flatMap((v) => [
        v.id,
        v.title,
        v.title_tw,
        v.content,
        v.content_tw,
        v.author_name,
        v.author_name_tw,
        v.author_id,
        v.classify,
      ])

      try {
        await client.query('BEGIN')
        await client.query(queryText, queryValues)
        await client.query('COMMIT')
        console.log(`成功导入 ${i * 1000} ~ ${(i + 1) * 1000} 条`)
      } catch (e) {
        await client.query('ROLLBACK')
        console.error(`导入第 ${i} 段失败：`, e.message)
      }
    }
  }
}

async function main() {
  await client.connect()
  await createTables()

  // console.log('导入唐诗宋诗作者数据...')
  // await importTangSongPoemAuthors()

  // console.log('导入宋词作者数据...')
  // await importSongCiAuthors()

  console.log('导入唐诗...')
  await insertTangPoems()

  console.log('导入宋诗...')
  await insertSongPoems()

  console.log('导入宋词...')
  await insertSongCi()

  console.log('导入完成!')
  await client.end()
}

main().catch((err) => {
  console.error('发生错误:', err)
  client.end()
})
