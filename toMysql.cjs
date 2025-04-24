const mysql = require('mysql2')
const OpenCC = require('opencc-js')
const { v4: uuidv4 } = require('uuid')

// 数据库连接配置
const dbConfig = {
  host: 'localhost',
  port: '3306',
  user: 'root',
  password: '123456',
}

// 导入诗词
// 由于chinese-poetry中的数据多为繁体，所以需要转换一下
// 繁体转简体正确度尚可（“乾隆”不用转换，会出错）
const converter = OpenCC.Converter({ from: 'tw', to: 'cn' })
const convertTW2CN = (s) => {
  if (s === '乾隆') return s
  return typeof s === 'string' ? converter(s) : s
}
// * 唐诗
const importTangPoems = async (connection) => {
  console.log(`=============唐诗导入开始============`)
  const startTime = Date.now() // 记录开始时
  // classify： 诗、词、曲、赋、骈文
  connection.query(`
    CREATE TABLE IF NOT EXISTS poem_tang (
      id CHAR(36) NOT NULL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_TW VARCHAR(255),
      content TEXT NOT NULL,
      content_TW TEXT,
      author VARCHAR(255) NOT NULL,
      author_TW VARCHAR(255),
      \`classify\` VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      deleted_at TIMESTAMP NULL DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  `)

  for (let i = 0; i < 58; i++) {
    const poems = require(`chinese-poetry/chinese-poetry/json/poet.tang.${
      i * 1000
    }.json`)
    const values = poems.map((poem) => [
      uuidv4(),
      convertTW2CN(poem.title),
      poem.title,
      Array.isArray(poem.paragraphs)
        ? poem.paragraphs.map((seg) => convertTW2CN(seg)).join('\n')
        : convertTW2CN(poem.paragraphs),
      Array.isArray(poem.paragraphs)
        ? poem.paragraphs.join('\n')
        : poem.paragraphs,
      convertTW2CN(poem.author),
      poem.author,
      '诗',
    ])

    const sql =
      'INSERT INTO poem_tang (id, title, title_TW, content, content_TW, author, author_TW, classify) VALUES ?'

    await new Promise((resolve, reject) => {
      connection.query(sql, [values], (err) => {
        if (err) return reject(err)
        console.log(`成功导入: ${i * 1000}~${(i + 1) * 1000}条数据`)
        resolve()
      })
    })
  }

  const endTime = Date.now() // 记录结束时间
  console.log(`=============唐诗导入结束============`)
  console.log(`耗时: ${Math.floor((endTime - startTime) / 1000)} 秒`)
}

// * 宋诗
const importSongPoems = async (connection) => {
  // classify： 诗、词、曲、赋、骈文
  connection.query(`
    CREATE TABLE IF NOT EXISTS poem_song (
      id CHAR(36) NOT NULL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_TW VARCHAR(255),
      content TEXT NOT NULL,
      content_TW TEXT,
      author VARCHAR(255) NOT NULL,
      author_TW VARCHAR(255),
      \`classify\` VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      deleted_at TIMESTAMP NULL DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  `)

  console.log(`=============宋诗导入开始============`)
  const startTime = Date.now() // 记录开始时
  for (let i = 0; i < 255; i++) {
    const poems = require(`chinese-poetry/chinese-poetry/json/poet.song.${
      i * 1000
    }.json`)
    const values = poems.map((poem) => [
      uuidv4(),
      convertTW2CN(poem.title),
      poem.title,
      Array.isArray(poem.paragraphs)
        ? poem.paragraphs.map((seg) => convertTW2CN(seg)).join('\n')
        : convertTW2CN(poem.paragraphs),
      Array.isArray(poem.paragraphs)
        ? poem.paragraphs.join('\n')
        : poem.paragraphs,
      convertTW2CN(poem.author),
      poem.author,
      '诗',
    ])

    const sql =
      'INSERT INTO poem_song (id, title, title_TW, content, content_TW, author, author_TW, classify) VALUES ?'

    await new Promise((resolve, reject) => {
      connection.query(sql, [values], (err) => {
        if (err) return reject(err)
        console.log(`成功导入: ${i * 1000}~${(i + 1) * 1000}条数据`)
        resolve()
      })
    })
  }
  const endTime = Date.now() // 记录结束时间
  console.log(`=============宋诗导入结束============`)
  console.log(`耗时: ${Math.floor((endTime - startTime) / 1000)} 秒`)
}

// * 宋词
const importSongCi = async (connection) => {
  // classify： 诗、词、曲、赋、骈文
  connection.query(`
    CREATE TABLE IF NOT EXISTS ci_song (
      id CHAR(36) NOT NULL PRIMARY KEY,
      title VARCHAR(255) NOT NULL,
      title_TW VARCHAR(255),
      content TEXT NOT NULL,
      content_TW TEXT,
      author VARCHAR(255) NOT NULL,
      author_TW VARCHAR(255),
      \`classify\` VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      deleted_at TIMESTAMP NULL DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  `)

  console.log(`=============宋词导入开始============`)
  const startTime = Date.now() // 记录开始时
  for (let i = 0; i < 22; i++) {
    const cis = require(`chinese-poetry/chinese-poetry/ci/ci.song.${
      i * 1000
    }.json`)
    const values = cis.map((ci) => [
      uuidv4(),
      ci.rhythmic,
      Array.isArray(ci.paragraphs) ? ci.paragraphs.join('\n') : ci.paragraphs,
      ci.author,
      '词',
    ])

    const sql =
      'INSERT INTO ci_song (id, title, content, author, classify) VALUES ?'

    await new Promise((resolve, reject) => {
      connection.query(sql, [values], (err) => {
        if (err) return reject(err)
        console.log(`成功导入: ${i * 1000}~${(i + 1) * 1000}条数据`)
        resolve()
      })
    })
  }
  const endTime = Date.now() // 记录结束时间
  console.log(`=============宋词导入结束============`)
  console.log(`耗时: ${Math.floor((endTime - startTime) / 1000)} 秒`)
}

// * 导入作者
const importAuthors = async (connection) => {
  connection.query(`
  CREATE TABLE IF NOT EXISTS author (
    id CHAR(36) NOT NULL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    name_TW VARCHAR(255),
    introduction TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    introduction_TW TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`)

  const author_tang = require(`chinese-poetry/chinese-poetry/json/authors.tang.json`)
  const author_song = require(`chinese-poetry/chinese-poetry/json/authors.song.json`)
  ;[...author_tang, ...author_song].forEach(async (item) => {
    const sql = `
      INSERT INTO author (id, name, name_TW, introduction, introduction_TW, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON DUPLICATE KEY UPDATE 
        name = VALUES(name),
        name_TW = VALUES(name_TW),
        introduction = VALUES(introduction),
        introduction_TW = VALUES(introduction_TW),
        updated_at = CURRENT_TIMESTAMP
    `

    await new Promise((resolve, reject) => {
      connection.query(
        sql,
        [
          uuidv4(),
          convertTW2CN(item.name),
          item.name,
          convertTW2CN(item.desc),
          item.desc,
        ],
        (err) => {
          if (err) return reject(err)
          console.log(`成功导入或更新作者: ${item.name}`)
          resolve()
        }
      )
    })
  })
}

async function main() {
  const connection = mysql.createConnection(dbConfig)
  connection.connect()
  // # 创建数据库（如果不存在）
  connection.query(
    `CREATE DATABASE IF NOT EXISTS digua_poetry CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`,
    (err) => {
      if (err) throw err
    }
  )

  // # 选择数据库
  connection.query(`USE digua_poetry;`)

  // # 创建表

  // # 导入诗词数据
  await importAuthors(connection)
  // await importTangPoems(connection)
  // await importSongPoems(connection)
  // await importSongCi(connection)

  console.log('导入完成')
  connection.end()
}

main().catch(console.error)
