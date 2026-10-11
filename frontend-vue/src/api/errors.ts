/** RFC 7807 problem details as returned by the gateway (docs/architecture.md section 6.1). */
export interface ProblemDetails {
  type: string
  title: string
  status: number
  detail?: string
  instance?: string
  request_id?: string
}

export class ApiError extends Error {
  readonly status: number
  readonly problem: ProblemDetails

  constructor(problem: ProblemDetails) {
    super(problem.detail ?? problem.title)
    this.name = 'ApiError'
    this.status = problem.status
    this.problem = problem
  }

  get requestId(): string | undefined {
    return this.problem.request_id
  }
}

function isProblem(body: unknown): body is ProblemDetails {
  return (
    typeof body === 'object' &&
    body !== null &&
    typeof (body as ProblemDetails).title === 'string' &&
    typeof (body as ProblemDetails).status === 'number'
  )
}

/**
 * Builds an ApiError from a failed Response. Falls back to a synthetic
 * problem when the body is not problem+json (e.g. a proxy error page).
 */
export async function toApiError(res: Response): Promise<ApiError> {
  let body: unknown
  try {
    body = await res.json()
  } catch {
    body = undefined
  }
  if (isProblem(body)) return new ApiError(body)
  return new ApiError({
    type: 'about:blank',
    title: res.statusText || 'Request failed',
    status: res.status,
    request_id: res.headers.get('X-Request-ID') ?? undefined,
  })
}
