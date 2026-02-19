import rawLog from "@bksLogger"
import { AthenaOptions, IDbConnectionDatabase, IamAuthType } from "@/lib/db/types"
import {
  AthenaClient as AWSAthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  ListDatabasesCommand,
  ListTableMetadataCommand,
  QueryExecutionState,
} from "@aws-sdk/client-athena"
import { fromIni } from "@aws-sdk/credential-providers"
import {
  BaseQueryResult,
  BasicDatabaseClient,
  ExecutionContext,
  QueryLogOptions
} from "@/lib/db/clients/BasicDatabaseClient"
import {
  BksField,
  BksFieldType,
  CancelableQuery,
  DatabaseFilterOptions,
  ExtendedTableColumn,
  FilterOptions,
  NgQueryResult,
  OrderBy,
  PrimaryKeyColumn,
  Routine,
  SchemaFilterOptions,
  StreamResults,
  SupportedFeatures,
  TableChanges,
  TableColumn,
  TableFilter,
  TableIndex,
  TableOrView,
  TableProperties,
  TableResult,
  TableTrigger
} from "@/lib/db/models"
import { TrinoData } from "@shared/lib/dialects/trino"
import _ from "lodash"
import {
  createCancelablePromise,
  joinFilters
} from "@/common/utils"
import {
  AlterTableSpec,
  TableKey
} from "@shared/lib/dialects/models"
import { IdentifyResult } from "sql-query-identifier/lib/defines"
import { errors } from "@/lib/errors"
import { IDbConnectionServer } from "@/lib/db/backendTypes"
import { ChangeBuilderBase } from "@shared/lib/sql/change_builder/ChangeBuilderBase"

interface AthenaResult extends BaseQueryResult {
  info?: any,
  length?: number,
  queryExecutionId?: string
}

const log = rawLog.scope("athena")
const knex = null
const athenaContext = {
  getExecutionContext(): ExecutionContext {
    return null;
  },
  logQuery(_query: string, _options: QueryLogOptions, _context: ExecutionContext): Promise<number | string> {
    return null
  }
}

const POLL_INTERVAL_MS = 500
const MAX_POLL_TIME_MS = 300000 // 5 minutes

export class AthenaClient extends BasicDatabaseClient<AthenaResult> {
  version: string
  client: AWSAthenaClient
  supportsTransaction: boolean
  athenaOptions: AthenaOptions
  catalog: string
  activeDatabase: string

  constructor(server: IDbConnectionServer, database: IDbConnectionDatabase) {
    super(knex, athenaContext, server, database)
    this.dialect = "generic"
    this.readOnlyMode = server?.config?.readOnlyMode || false
    this.athenaOptions = server?.config?.athenaOptions || {}

    // The catalog is always from the config ("Default Catalog" form field)
    this.catalog = server?.config?.defaultDatabase || 'AwsDataCatalog'

    // When user switches databases from the sidebar, database.database is the
    // selected Athena database name. On initial connect, it equals the catalog.
    const dbName = database?.database
    if (dbName && dbName !== this.catalog) {
      this.activeDatabase = dbName
    } else {
      this.activeDatabase = this.athenaOptions.database || 'default'
    }
  }

  private async resolveCredentials() {
    const iamOptions = this.server.config.iamAuthOptions
    if (!iamOptions || !iamOptions.iamAuthenticationEnabled) {
      // Use default credential chain (env vars, instance profile, etc.)
      return undefined
    }

    if (iamOptions.authType === IamAuthType.Key) {
      if (iamOptions.accessKeyId && iamOptions.secretAccessKey) {
        return {
          accessKeyId: iamOptions.accessKeyId,
          secretAccessKey: iamOptions.secretAccessKey,
        }
      }
    }

    if (iamOptions.authType === IamAuthType.File || iamOptions.authType === IamAuthType.CLI) {
      return fromIni({
        profile: iamOptions.awsProfile || "default",
      })
    }

    return undefined
  }

  async connect(): Promise<void> {
    await super.connect()

    const iamOptions = this.server.config.iamAuthOptions || {}
    const region = iamOptions.awsRegion || 'us-east-1'
    const credentials = await this.resolveCredentials()

    const clientConfig: any = { region }
    if (credentials) {
      clientConfig.credentials = credentials
    }

    this.client = new AWSAthenaClient(clientConfig)

    // Verify connection by listing databases
    try {
      const command = new ListDatabasesCommand({
        CatalogName: this.catalog,
      })
      await this.client.send(command)
    } catch (err) {
      log.error("Failed to connect to Athena:", err)
      throw new Error(`Failed to connect to Amazon Athena: ${err.message}`)
    }

    this.version = "Amazon Athena"
    this.supportsTransaction = false
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      this.client.destroy()
      this.client = null
    }
    await super.disconnect()
  }

  async versionString(): Promise<string> {
    return this.version
  }

  private async startAndWaitForQuery(sql: string): Promise<{ columns: any[], rows: Record<string, any>[] }> {
    const params: any = {
      QueryString: sql,
      QueryExecutionContext: {
        Catalog: this.catalog,
      },
    }

    if (this.activeDatabase) {
      params.QueryExecutionContext.Database = this.activeDatabase
    }

    if (this.athenaOptions.s3OutputLocation || this.athenaOptions.workgroup) {
      params.ResultConfiguration = {}
      if (this.athenaOptions.s3OutputLocation) {
        params.ResultConfiguration.OutputLocation = this.athenaOptions.s3OutputLocation
      }
    }

    if (this.athenaOptions.workgroup) {
      params.WorkGroup = this.athenaOptions.workgroup
    }

    // Start the query
    const startCommand = new StartQueryExecutionCommand(params)
    const startResult = await this.client.send(startCommand)
    const queryExecutionId = startResult.QueryExecutionId

    // Poll for completion
    const startTime = Date.now()
    while (true) {
      const getCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId })
      const execResult = await this.client.send(getCommand)
      const state = execResult.QueryExecution?.Status?.State

      if (state === QueryExecutionState.SUCCEEDED) {
        break
      } else if (state === QueryExecutionState.FAILED) {
        const reason = execResult.QueryExecution?.Status?.StateChangeReason || 'Unknown error'
        throw new Error(`Athena query failed: ${reason}`)
      } else if (state === QueryExecutionState.CANCELLED) {
        throw new Error('Athena query was cancelled')
      }

      if (Date.now() - startTime > MAX_POLL_TIME_MS) {
        throw new Error('Athena query timed out')
      }

      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS))
    }

    // Fetch results with pagination
    const allRows: Record<string, any>[] = []
    let columns: any[] = []
    let nextToken: string | undefined
    let isFirstPage = true

    do {
      const getResultsCommand = new GetQueryResultsCommand({
        QueryExecutionId: queryExecutionId,
        NextToken: nextToken,
      })
      const resultsResponse = await this.client.send(getResultsCommand)

      if (columns.length === 0 && resultsResponse.ResultSet?.ResultSetMetadata?.ColumnInfo) {
        columns = resultsResponse.ResultSet.ResultSetMetadata.ColumnInfo.map(col => ({
          name: col.Name,
          type: col.Type,
        }))
      }

      const resultRows = resultsResponse.ResultSet?.Rows || []

      // First row of the first page is the header row - skip it
      const dataRows = isFirstPage ? resultRows.slice(1) : resultRows
      isFirstPage = false

      for (const row of dataRows) {
        const obj: Record<string, any> = {}
        const data = row.Data || []
        for (let i = 0; i < columns.length; i++) {
          obj[columns[i].name] = data[i]?.VarCharValue ?? null
        }
        allRows.push(obj)
      }

      nextToken = resultsResponse.NextToken
    } while (nextToken)

    return { columns, rows: allRows }
  }

  async rawExecuteQuery(sql: string): Promise<AthenaResult> {
    try {
      const cleanSql = sql.trim().replace(/;$/, '')
      const { columns, rows } = await this.startAndWaitForQuery(cleanSql)

      return {
        columns,
        rows,
        arrayMode: false
      }
    } catch (err) {
      log.error(err)
      throw err
    }
  }

  async executeQuery(queryText: string): Promise<NgQueryResult[]> {
    const queries = queryText.trim().split(';')
    const results: NgQueryResult[] = await Promise.all(
      queries
        .filter(q => q.trim() !== '')
        .map(async q => {
          const { rows, columns } = await this.driverExecuteSingle(q)
          const fields = rows.length === 0 ? [] : columns.map(c => ({ ...c, id: c.name }))
          return {
            fields,
            rows,
            rowCount: rows.length,
            affectedRows: 0,
            command: 'SELECT'
          } satisfies NgQueryResult
        })
    )
    return results
  }

  async query(queryText: string): Promise<CancelableQuery> {
    const cancelable = createCancelablePromise(errors.CANCELED_BY_USER)
    return {
      execute: async (): Promise<NgQueryResult[]> => {
        try {
          const data = await Promise.race([
            cancelable.wait(),
            this.executeQuery(queryText),
          ])
          if (!data) return []
          return data
        } catch (err) {
          if (cancelable.canceled) {
            err.sqlectronError = "CANCELED_BY_USER"
          }
          throw err
        } finally {
          cancelable.discard()
        }
      },
      cancel: async (): Promise<void> => {
        // Athena queries can't easily be cancelled through this interface
      },
    }
  }

  async alterTable(_change: AlterTableSpec): Promise<void> {
    log.info("Athena doesn't support altering tables")
    return null
  }

  async getPrimaryKeys(): Promise<PrimaryKeyColumn[]> {
    return []
  }

  async getPrimaryKey(_table: string, _schema?: string): Promise<string | null> {
    return null
  }

  async selectTop(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    schema: string,
    selects: string[],
  ): Promise<TableResult> {
    const columns = await this.listTableColumns(table, schema)
    let selectFields = [...selects]
    if (!selects || selects?.length === 0 || (selects?.length === 1 && selects[0] === '*')) {
      selectFields = columns.map((v) => v.columnName)
    }

    const queries = this.buildSelectTopQuery(
      table,
      offset,
      limit,
      orderBy,
      filters,
      "total",
      columns,
      selectFields,
      schema
    )

    const { query } = queries
    const result = await this.driverExecuteSingle(query)
    const fields = result.columns ? result.columns.map(c => ({
      name: c.name,
      bksType: 'UNKNOWN' as BksFieldType
    })) : []
    return {
      result: result.rows || [],
      fields
    }
  }

  async selectTopSql(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    schema: string,
    selects: string[]
  ): Promise<string> {
    const columns = await this.listTableColumns(table, schema)
    const { query } = this.buildSelectTopQuery(
      table,
      offset,
      limit,
      orderBy,
      filters,
      "total",
      columns,
      selects,
      schema
    )
    return query
  }

  async getTableProperties(
    _table: string,
    _schema?: string
  ): Promise<TableProperties> {
    return null
  }

  async getOutgoingKeys(_table: string, _schema?: string): Promise<TableKey[]> {
    return []
  }

  async getIncomingKeys(_table: string, _schema?: string): Promise<TableKey[]> {
    return []
  }

  async listTableTriggers(
    _table: string,
    _schema?: string
  ): Promise<TableTrigger[]> {
    return []
  }

  async listTableIndexes(
    _table: string,
    _schema?: string
  ): Promise<TableIndex[]> {
    return []
  }

  async listViews(
    _filter: FilterOptions = { schema: "public" }
  ): Promise<TableOrView[]> {
    return []
  }

  async executeApplyChanges(_changes: TableChanges): Promise<any[]> {
    log.info("Athena doesn't support changing data")
    return null
  }

  async dropElement(): Promise<void> {
    log.info("Athena doesn't support dropping elements")
    return null
  }

  async listDatabases(_filter?: DatabaseFilterOptions): Promise<string[]> {
    try {
      const command = new ListDatabasesCommand({
        CatalogName: this.catalog,
      })
      const result = await this.client.send(command)
      return (result.DatabaseList || []).map(db => db.Name)
    } catch (err) {
      log.error("Failed to list databases:", err)
      throw err
    }
  }

  async listSchemas(_filter: SchemaFilterOptions): Promise<string[]> {
    return await this.listDatabases()
  }

  async listTables(filter?: FilterOptions): Promise<TableOrView[]> {
    const database = filter?.schema || this.activeDatabase

    try {
      const tables: TableOrView[] = []
      let nextToken: string | undefined

      do {
        const command = new ListTableMetadataCommand({
          CatalogName: this.catalog,
          DatabaseName: database,
          NextToken: nextToken,
        })
        const result = await this.client.send(command)

        for (const table of result.TableMetadataList || []) {
          tables.push({
            schema: database,
            name: table.Name,
            entityType: 'table' as const
          })
        }

        nextToken = result.NextToken
      } while (nextToken)

      return tables
    } catch (err) {
      log.error("Failed to list tables:", err)
      throw err
    }
  }

  async listTableColumns(table: string, schema: string): Promise<ExtendedTableColumn[]> {
    const database = schema || this.activeDatabase
    const sql = `
      SELECT
        *
      FROM ${this.wrapIdentifier(this.catalog)}.information_schema.columns
      WHERE table_schema = '${database}'
        AND table_name = '${table}'
      ORDER BY ordinal_position
    `
    const result = await this.driverExecuteSingle(sql)
    return result.rows.map((row) => {
      const hasDefault = row.column_default != null

      return {
        schemaName: row.table_schema,
        tableName: row.table_name,
        columnName: row.column_name,
        dataType: row.data_type,
        ordinalPosition: row.ordinal_position,
        defaultValue: row.column_default,
        hasDefault,
        comment: row.comment,
        primaryKey: false,
        nullable: row.is_nullable,
        bksField: this.parseTableColumn(row),
      }
    })
  }

  async createDatabase(): Promise<string> {
    log.debug("Athena doesn't support creating databases through this interface")
    return null
  }

  async truncateElementSql() {
    log.debug("Athena doesn't support truncation")
    return null
  }

  async duplicateTable(): Promise<void> {
    log.debug("Athena doesn't support duplicating tables")
    return null
  }

  async duplicateTableSql(): Promise<string> {
    log.debug("Athena doesn't support duplicating tables")
    return null
  }

  async setElementNameSql(): Promise<string> {
    log.debug("Athena doesn't support renaming elements")
    return null
  }

  async getBuilder(_table: string, _schema?: string): Promise<ChangeBuilderBase> {
    log.debug("Athena doesn't support change builders")
    return null
  }

  parseFields(fields: any[]) {
    return fields.map(column => ({
      dataType: column.type,
      id: column.name,
      name: column.name
    }))
  }

  async supportedFeatures(): Promise<SupportedFeatures> {
    return {
      customRoutines: false,
      comments: false,
      properties: false,
      partitions: false,
      editPartitions: false,
      backups: false,
      backDirFormat: false,
      restore: false,
      indexNullsNotDistinct: false,
      transactions: false,
      filterTypes: ['standard']
    }
  }

  async listRoutines(_filter?: FilterOptions): Promise<Routine[]> {
    return []
  }

  async listMaterializedViewColumns(): Promise<TableColumn[]> {
    return []
  }

  async getTableReferences(
    _table: string,
    _schema?: string
  ): Promise<string[]> {
    return []
  }

  async getQuerySelectTop(
    table: string,
    limit: number,
    _schema?: string
  ): Promise<string> {
    return `SELECT * FROM ${TrinoData.wrapIdentifier(
      table
    )} LIMIT ${limit}`
  }

  async listMaterializedViews(_filter?: FilterOptions): Promise<TableOrView[]> {
    return []
  }

  async listCharsets(): Promise<string[]> {
    return []
  }

  async getDefaultCharset(): Promise<string> {
    return ""
  }

  async listCollations(_charset: string): Promise<string[]> {
    return []
  }

  async createDatabaseSQL(): Promise<string> {
    throw new Error("Method not implemented.")
  }

  async getTableCreateScript(_table: string, _schema?: string): Promise<string> {
    return ''
  }

  async getViewCreateScript(_view: string, _schema?: string): Promise<string[]> {
    return []
  }

  async getRoutineCreateScript(): Promise<string[]> {
    return []
  }

  async setTableDescription(): Promise<string> {
    return ''
  }

  async truncateAllTables(_schema?: string): Promise<void> {
    log.debug("Athena doesn't support truncation")
  }

  async getTableLength(table: string, schema: string): Promise<number> {
    const database = schema || this.activeDatabase
    const result = await this.driverExecuteSingle(
      `SELECT count(*) as count FROM ${this.wrapIdentifier(this.catalog)}.${this.wrapIdentifier(database)}.${this.wrapIdentifier(table)}`
    )

    const [row] = result.rows as { count: string }[]
    return Number(row.count)
  }

  async selectTopStream(): Promise<StreamResults> {
    return {
      columns: [],
      totalRows: 0,
      cursor: null
    }
  }

  queryStream(_query: string, _chunkSize: number): Promise<StreamResults> {
    throw new Error("Method not implemented.")
  }

  wrapIdentifier(value: string): string {
    return TrinoData.wrapIdentifier(value)
  }

  wrapDynamicLiteral(value: any): string {
    if (value == null) return 'NULL'
    if (typeof value === 'number' || /^[+-]?([0-9]*[.])?[0-9]+$/.test(value)) {
      return value.toString()
    }
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE'
    }
    return `'${value.toString().replace(/'/g, "''")}'`
  }

  buildFilterString(filters: TableFilter[], columns = []) {
    let fullFilterString = ""

    if (filters && Array.isArray(filters) && filters.length > 0) {
      const filtersWithoutParams: string[] = []

      filters.forEach((item) => {
        const column = columns.find((c) => c.columnName === item.field)
        const field = column?.dataType?.toUpperCase().includes("BINARY")
          ? `HEX(${TrinoData.wrapIdentifier(item.field)})`
          : TrinoData.wrapIdentifier(item.field)

        const op = item.type.toUpperCase()
        const val = item.value

        if (op === "IS NULL" || op === "IS NOT NULL") {
          filtersWithoutParams.push(`${field} ${op}`)
          return
        }

        if (op === "IN" && Array.isArray(val)) {
          const values = val
            .map((v) => this.wrapDynamicLiteral(v))
            .join(", ")
          filtersWithoutParams.push(`${field} IN (${values})`)
          return
        }

        if (
          ["=", "!=", "<", "<=", ">", ">=", "LIKE", "ILIKE"].includes(op) &&
          val != null
        ) {
          const literal = this.wrapDynamicLiteral(val)
          filtersWithoutParams.push(`${field} ${op} ${literal}`)
          return
        }
      })

      fullFilterString = "WHERE " + joinFilters(filtersWithoutParams, filters)
    }

    return {
      fullFilterString,
    }
  }

  buildSelectTopQuery(
    table: string,
    offset: number,
    limit: number,
    orderBy: OrderBy[],
    filters: string | TableFilter[],
    countTitle = "total",
    columns = [],
    selects = ["*"],
    schema
  ) {
    log.info("building selectTop for", table, offset, limit, orderBy, selects, schema)

    const safeOffset = Number.isFinite(offset) ? offset : 0
    const safeLimit = Number.isFinite(limit) ? limit : 100
    const usePagination = Number.isFinite(limit)
    const selectsArr = !Array.isArray(selects) || selects.length === 0 ? ['*'] : selects

    let rowNumberOrderClause = ""

    if (orderBy && orderBy.length > 0) {
      const orderByParts = orderBy.map((item: any) => {
        if (_.isObject(item)) {
          return `${TrinoData.wrapIdentifier(item["field"])} ${item["dir"].toUpperCase()}`
        } else {
          return TrinoData.wrapIdentifier(item)
        }
      })

      rowNumberOrderClause = "ORDER BY " + orderByParts.join(", ")
    } else {
      rowNumberOrderClause = "ORDER BY 1"
    }

    let filterString = ""
    let fullFilterString = ""
    if (_.isString(filters)) {
      filterString = fullFilterString = `WHERE ${filters}`
    } else {
      const filterBlob = this.buildFilterString(filters, columns)
      filterString = filterBlob.fullFilterString
      fullFilterString = filterBlob.fullFilterString
    }

    const wrappedSelects = selectsArr.map((s) => s === '*' ? s : TrinoData.wrapIdentifier(s)).join(", ")
    const database = schema || this.activeDatabase
    const wrappedTable = `${TrinoData.wrapIdentifier(database)}.${TrinoData.wrapIdentifier(table)}`

    const countSQL = `
      SELECT COUNT(*) AS ${countTitle}
      FROM ${wrappedTable}
      ${filterString}
    `

    const paginatedSQL = this.buildPaginatedQuery(wrappedTable, filterString, wrappedSelects, rowNumberOrderClause, usePagination, safeOffset, safeLimit)
    const fullSql = this.buildPaginatedQuery(TrinoData.wrapIdentifier(table), fullFilterString, wrappedSelects, rowNumberOrderClause, usePagination, safeOffset, safeLimit)

    return {
      query: paginatedSQL,
      fullQuery: fullSql,
      countQuery: countSQL,
      params: {},
    }
  }

  buildPaginatedQuery(tableRef: string, filter: string, wrappedSelects: string, rowNumberOrderClause: string, usePagination: boolean, safeOffset: number, safeLimit: number): string {
    return `
      WITH ranked AS (
        SELECT
          ${wrappedSelects},
          ROW_NUMBER() OVER (${rowNumberOrderClause}) AS rownum
        FROM ${this.catalog}.${tableRef}
        ${filter}
      )
      SELECT *
      FROM ranked
      ${usePagination ? `WHERE rownum > ${safeOffset} AND rownum <= ${safeOffset + safeLimit}` : ""}
    `
  }

  protected violatesReadOnly(statements: IdentifyResult[], options: any = {}) {
    return (
      super.violatesReadOnly(statements, options) ||
      (this.readOnlyMode && options.insert)
    )
  }

  parseTableColumn(column: TableColumn): BksField {
    return { name: column.columnName, bksType: "UNKNOWN" }
  }
}
