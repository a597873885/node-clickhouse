const { DataTypes } = require('./consts')
const util = require('./utils')
// 是否打印sql
class NodeClickHouse {
  constructor({
    schemaPath,
    client,
    showSql
  }) {
    this.schema = require(schemaPath)
    this.client = client
    this.showSql = showSql
  }
  static QueryTypes = {
    SELECT: "SELECT",
    INSERT: "INSERT",
    DELETE: "DELETE"
  }
  /**
   * 查询sql
   * 如果没有结果，返回空数组
   */
  static async query(sql) {
    // 是否打印sql
    this.showSql && console.log("查询：".blue, sql)
    const rows = await this.client.query({
      query: sql,
      format: 'JSONEachRow',
    }).catch((e) => {
      console.error(e)
      
    })
    if (!rows) return []
    return await rows.json()
  }

  /**
   * 执行sql
   */
  static async execSql(sql) {
    // 是否打印sql
    this.showSql && console.log("执行sql：".blue, sql)
    return await this.client.command({ query: sql })
  }


  static checkColumnType(structure, columnName) {
    try {
      let typeStr = ""
      const type = structure[columnName].type
      // UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256
      switch(type) {
        case "UInt8":
        case "UInt16":
        case "UInt32":
        case "UInt64":
        case "UInt128":
        case "UInt256":
        case "Int8":
        case "Int16":
        case "Int32":
        case "Int64":
        case "Int128":
        case "Int256":
          typeStr = "number"
          break
        case "STRING":
        case "DATE":
        case "DateTime":
          typeStr = "string"
          break
        default:
          typeStr = "string"
          break
      }
      return typeStr
    } catch(e) {
      console.log(e)
      console.log(structure, columnName)
    }
    
  }
  /**
   * data可以覆盖Columns的字段
   */
  async createTable(data = {}) {
    const finalColumns = { ...this.schema.Columns, ...data }
    const { tableName, structure, engine = "ENGINE MergeTree()", indexSql, orderBy = "ORDER BY (dataId)" } = finalColumns
    let sqlColumns = ""
    // 处理字段
    for (let key in structure) {
      const { type, field, allowNull = true, comment = "", defaultValue = "", dataModel = ""} = structure[key]
      const nullSql = allowNull ? "" : "NOT NULL"
      const commentSql = comment ? `COMMENT "${comment}"` : ""
      const dataModelSql = dataModel ? dataModel : ""
      const defaultValueSql = typeof defaultValue === "number" ? `DEFAULT ${defaultValue}` : ""
      sqlColumns += "`" + field + "`" + ` ${type} ${nullSql} ${dataModelSql} ${defaultValueSql} ${commentSql}, `
    }
    const lastSplitIndex = sqlColumns.lastIndexOf(", ")
    sqlColumns = sqlColumns.substring(0, lastSplitIndex)

    sqlColumns = `( ${sqlColumns}
      ${indexSql} )`
    const sql = `CREATE TABLE IF NOT EXISTS ${tableName}
    ${sqlColumns}
    ${engine}
    ${orderBy}
    `
    // 是否打印sql
    this.showSql && console.log("建表：".green, sql)
    return await this.client.command({ query: sql })
  }

  /**
   * doris 插入数据的组合操作
   */
  async create(data, newTableName = "") {
    const columns = newTableName ? {...this.schema.Columns, tableName: newTableName} : this.schema.Columns
    const { structure, tableName } = columns
    let keySql = ""
    let valSql = ""
    for (let key in structure) {
      const columnInfo = structure[key]
      if (data[key]) {
        const val = data[key]
        keySql += `${key}, `
        valSql += NodeClickHouse.checkColumnType(structure, key) === "number" ? `${val}, ` : `'${val}', `
      } else if (key === "createdAt" || key === "updatedAt") {
        keySql += `${key}, `
        valSql += NodeClickHouse.checkColumnType(structure, key) === "number" ? `0, ` : `'${columnInfo.get()}', `
      } else if (structure[key].type === DataTypes.UUID) {
        keySql += `${key}, `
        valSql += `generateUUIDv4(), `
      }
    }
    keySql = "(" + keySql.substring(0, keySql.lastIndexOf(", ")) + ")"
    valSql = "(" + valSql.substring(0, valSql.lastIndexOf(", ")) + ")"
    const sql = `INSERT INTO ${tableName} ${keySql} VALUES ${valSql}`
    // return await NodeClickHouse.query(sql, { type: NodeClickHouse.QueryTypes.INSERT})
    // 是否打印sql
    this.showSql && console.log("插入：".cyan, sql)
    return await this.client.command({ query: sql })
  }
  
  /**
   * doris 插入数据的组合操作，并返回结果
   */
  async createWithRes(data, newTableName = "") {
    const columns = newTableName ? {...this.schema.Columns, tableName: newTableName} : this.schema.Columns
    const { structure, tableName } = columns
    let keySql = ""
    let valSql = ""
    let uuidStr = util.getUuid();
    for (let key in structure) {
      const columnInfo = structure[key]
      const val = data[key]
      if (val) {
        keySql += `${key}, `
        valSql += NodeClickHouse.checkColumnType(structure, key) === "number" ? `${val}, ` : `'${val}', `
      } else if (key === "createdAt" || key === "updatedAt") {
        keySql += `${key}, `
        valSql += NodeClickHouse.checkColumnType(structure, key) === "number" ? `0, ` : `'${columnInfo.get()}', `
      } else if (structure[key].type === DataTypes.UUID) {
        keySql += `${key}, `
        // valSql += `generateUUIDv4(), `
        valSql += "'" + uuidStr + "',"
      }
    }
    keySql = "(" + keySql.substring(0, keySql.lastIndexOf(", ")) + ")"
    valSql = "(" + valSql.substring(0, valSql.lastIndexOf(", ")) + ")"
    const sql = `INSERT INTO ${tableName} ${keySql} VALUES ${valSql}`
    // return await NodeClickHouse.query(sql, { type: NodeClickHouse.QueryTypes.INSERT})
    // 是否打印sql
    this.showSql && console.log("插入：".cyan, sql)
    await this.client.command({ query: sql });
  
    let whereSql = " WHERE id = '" + uuidStr + "'"
    if(tableName === 'Config' || tableName === 'config' || 
       tableName === 'User' || tableName === 'user' || 
       tableName === 'Message' || tableName === 'message' || 
       tableName === 'Team' || tableName === 'team'){
      whereSql = " WHERE dataId = '" + uuidStr + "'"
    }
    // 查询
    let querySql = "select * from " + `${tableName}` + whereSql
    const res =  await NodeClickHouse.query(querySql)
    if (res) {
      return res[0]
    } else {
      return 0
    }
  }

  /**
   * doris 更新数据
   */
  async update(data, query, newTableName = "") {
    const columns = newTableName ? {...this.schema.Columns, tableName: newTableName} : this.schema.Columns
    const { structure, tableName } = columns
    const { where, fields } = query
    let setSql = ""
    fields.forEach((fieldName) => {
      if (structure[fieldName]) {
        const valSql = NodeClickHouse.checkColumnType(structure, fieldName) === "number" ? data[fieldName] : `'${data[fieldName]}'`
        setSql += `${fieldName}=${valSql}, `
      }
    })
    setSql += `updatedAt='${structure.updatedAt.get()}'`

    let whereSql = where ? " WHERE 1=1 " : ""
    for (let key in where) {
      if (structure[key]) {
        const valSql = NodeClickHouse.checkColumnType(structure, key) === "number" ? where[key] : `'${where[key]}'`
        whereSql += `AND ${key}=${valSql} `
      }
    }
    // 更新sql
    const updateSql = `ALTER TABLE ${tableName} UPDATE
    ${setSql}
    ${whereSql}
    `
    // 是否打印sql
    this.showSql && console.log("更新：".red, updateSql)
    return await this.client.command({ query: updateSql })
  }

  /**
   * doris 更新数据, 如果没有数据，则插入
   */
  async updateWithInsert(data, query, newTableName = "") {
    const columns = newTableName ? {...this.schema.Columns, tableName: newTableName} : this.schema.Columns
    const { structure, tableName } = columns
    const { where, fields } = query
    let setSql = ""
    fields.forEach((fieldName) => {
      if (structure[fieldName]) {
        const valSql = NodeClickHouse.checkColumnType(structure, fieldName) === "number" ? data[fieldName] : `'${data[fieldName]}'`
        setSql += `${fieldName}=${valSql}, `
      }
    })
    setSql += `updatedAt='${structure.updatedAt.get()}'`

    let whereSql = where ? " WHERE 1=1 " : ""
    for (let key in where) {
      if (structure[key]) {
        const valSql = NodeClickHouse.checkColumnType(structure, key) === "number" ? where[key] : `'${where[key]}'`
        whereSql += `AND ${key}=${valSql} `
      }
    }
    // 更新sql
    const updateSql = `ALTER TABLE ${tableName} UPDATE
    ${setSql}
    ${whereSql}
    `
    // 先查询有没有结果，有则更新，没有则插入
    const queryRes = await this.findOne(query, newTableName)
    if (queryRes.length) {
      // 是否打印sql
      this.showSql && console.log("更新：".red, updateSql)
      return await this.client.command({ query: updateSql })
    } else {
      return await this.create({...data, ...where})
    }

  }

  /**
   * doris 更新数据
   */
  async findOne(query, newTableName = "") {
    const tableName = newTableName ? newTableName : this.schema.Columns.tableName
    const { where } = query

    let whereSql = where ? " WHERE 1=1 " : ""
    for (let key in where) {
      const valSql = NodeClickHouse.checkColumnType(this.schema.Columns.structure, key) === "number" ? where[key] : `'${where[key]}'`
      whereSql += `AND ${key}=${valSql} `
    }
    
    const sql = `SELECT * from ${tableName} ${whereSql}`
    const res =  await NodeClickHouse.query(sql)
    if (res) {
      return res[0]
    } else {
      return 0
    }
  }

  /**
   *  删除数据
   */
  async destroy(query, newTableName = "") {
    const tableName = newTableName ? newTableName : this.schema.Columns.tableName
    const { where } = query

    let whereSql = where ? " WHERE 1=1 " : ""
    for (let key in where) {
      const valSql = NodeClickHouse.checkColumnType(this.schema.Columns.structure, key) === "number" ? where[key] : `'${where[key]}'`
      whereSql += `AND ${key}=${valSql}`
    }
    
    const sql = `delete from ${tableName} ${whereSql}`
    return await NodeClickHouse.query(sql)
  }
}
module.exports = NodeClickHouse