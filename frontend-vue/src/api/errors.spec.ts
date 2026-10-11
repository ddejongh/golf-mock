import { describe, expect, it } from 'vitest'
import { ApiError, toApiError } from './errors'

describe('toApiError', () => {
  it('parses problem+json bodies', async () => {
    const res = new Response(
      JSON.stringify({ type: 'urn:x', title: 'Forbidden', status: 403, detail: 'operators only', request_id: 'r1' }),
      { status: 403, headers: { 'Content-Type': 'application/problem+json' } },
    )
    const err = await toApiError(res)
    expect(err).toBeInstanceOf(ApiError)
    expect(err.status).toBe(403)
    expect(err.message).toBe('operators only')
    expect(err.requestId).toBe('r1')
  })

  it('falls back when the body is not problem+json', async () => {
    const res = new Response('<html>bad gateway</html>', {
      status: 502,
      statusText: 'Bad Gateway',
      headers: { 'X-Request-ID': 'r2' },
    })
    const err = await toApiError(res)
    expect(err.status).toBe(502)
    expect(err.message).toBe('Bad Gateway')
    expect(err.requestId).toBe('r2')
  })
})
