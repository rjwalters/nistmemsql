export interface ErrorTest {
  id: string
  sql: string
  error: string
}

export interface ConformanceData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
  error_tests?: ErrorTest[]
}

export interface SQLLogicTestCategory {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
}

export interface SQLLogicTestData {
  total: number
  passed: number
  failed: number
  errors: number
  pass_rate: number
  categories: {
    select?: SQLLogicTestCategory
    evidence?: SQLLogicTestCategory
    index?: SQLLogicTestCategory
    random?: SQLLogicTestCategory
    ddl?: SQLLogicTestCategory
    other?: SQLLogicTestCategory
  }
}

export interface ConformanceReportState {
  data: ConformanceData | null
  sltData: SQLLogicTestData | null
  loading: boolean
  error: string | null
}
