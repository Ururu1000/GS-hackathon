// Privacy redaction and destination masking helpers shared by the UI and dev mock.

const SENSITIVE_KEY =
  /(?:^|[_-])(pass(?:word|wd)?|pwd|secret|token|api[-_]?key|auth(?:orization)?|cookie|cred(?:ential)?s?|private[-_]?key|access[-_]?key|session|ssn|tax[-_]?id|iban|account[-_]?num(?:ber)?|card[-_]?num(?:ber)?|cvv|cvc|pin|sms[-_]?code|otp|email|e[-_]?mail|phone|mobile|tel(?:ephone)?)(?:$|[_-])/i

const FORBIDDEN_KEYS = new Set(['__proto__', 'constructor', 'prototype'])

const BEARER_RE = /\bBearer\s+[A-Za-z0-9._~+/=-]{8,}/gi
const JWT_RE = /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g
const KV_SECRET_RE =
  /\b(password|passwd|secret|token|api[_-]?key|authorization|cookie)\s*([=:])\s*([^\s,;'"`]+)/gi
const EMAIL_RE = /\b([A-Za-z0-9._%+-])([A-Za-z0-9._%+-]*)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})\b/g
const PHONE_RE = /(?<!\w)(\+\d{1,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?){2,}\d{3,4}(?!\w)/g
const CTRL_RE = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g

export const isSensitiveKey = (key: string): boolean => SENSITIVE_KEY.test(key)

export function maskEmail(addr: string): string {
  const at = addr.indexOf('@')
  if (at <= 0) return '***'
  const local = addr.slice(0, at)
  const domain = addr.slice(at + 1)
  if (local.includes('***')) return addr
  return `${local.length > 1 ? local[0] : ''}***@${domain}`
}

export function maskPhone(addr: string): string {
  if (addr.includes('•••')) return addr
  const digits = addr.replace(/\D/g, '')
  if (digits.length < 4) return '••••'
  const last4 = digits.slice(-4)
  const prefix = addr.trim().match(/^\+\d{1,3}/)?.[0]
  return prefix ? `${prefix} •••• ••${last4}` : `•••${last4}`
}

/** Mask recipient email/phone so alert views don't broadcast full personal contact info. */
export function maskAddress(channel: string, address: string): string {
  const trimmed = address.trim()
  if (!trimmed || trimmed === '—' || trimmed.includes('***') || trimmed.includes('•••')) return trimmed
  if (channel === 'email' || trimmed.includes('@')) return maskEmail(trimmed)
  if (channel === 'sms' || channel === 'phone' || /^\+?[\d\s().-]{7,}$/.test(trimmed)) return maskPhone(trimmed)
  return trimmed
}

/** Scrub credentials, tokens, emails, phones and control characters from free text or outputs. */
export function redactText(text: string, maxLen?: number): string {
  let out = text
    .replace(CTRL_RE, '')
    .replace(BEARER_RE, 'Bearer [REDACTED]')
    .replace(JWT_RE, '[REDACTED_JWT]')
    .replace(KV_SECRET_RE, '$1$2[REDACTED]')
    .replace(EMAIL_RE, (_, first: string, rest: string, domain: string) => `${rest ? first : ''}***@${domain}`)
    .replace(PHONE_RE, (m) => {
      const digits = m.replace(/\D/g, '')
      // Avoid masking timestamps, IDs or short numbers; require at least 9 digits for phone numbers.
      return digits.length >= 9 && digits.length <= 15 ? maskPhone(m) : m
    })
  if (maxLen !== undefined && out.length > maxLen) out = `${out.slice(0, Math.max(0, maxLen - 1))}…`
  return out
}

/** Recursively redact sensitive keys and credential/PII patterns from an arbitrary JSON payload. */
export function redactValue(value: unknown, maxDepth = 12, depth = 0): unknown {
  if (depth > maxDepth) return '[TRUNCATED]'
  if (typeof value === 'string') return redactText(value)
  if (value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((item) => redactValue(item, maxDepth, depth + 1))
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (FORBIDDEN_KEYS.has(k)) continue
    out[k] = isSensitiveKey(k) ? '[REDACTED]' : redactValue(v, maxDepth, depth + 1)
  }
  return out
}
