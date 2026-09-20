// Low-level HTTP client for the TaskForge API. Every page/store reads data
// exclusively through this module — there is no hard-coded sample data.

type Query = Record<string, string | number | undefined>

function buildUrl(path: string, query?: Query): string {
  const cfg = useRuntimeConfig()
  const base = (cfg.public.apiBase as string) || ''
  const url = new URL(base + path, window.location.origin)
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== '') {
        url.searchParams.set(k, String(v))
      }
    }
  }
  // When base is empty we want a same-origin relative path.
  return base ? url.toString() : url.pathname + url.search
}

async function request<T>(method: string, path: string, body?: unknown, query?: Query): Promise<T> {
  const res = await fetch(buildUrl(path, query), {
    method,
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`
    try {
      const err = await res.json()
      if (err?.error?.message) detail = err.error.message
    } catch {
      /* ignore */
    }
    throw new Error(detail)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}

export const api = {
  get: <T>(path: string, query?: Query) => request<T>('GET', path, undefined, query),
  post: <T>(path: string, body?: unknown) => request<T>('POST', path, body ?? {}),
}
