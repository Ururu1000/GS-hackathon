/** Convert a displayed phone number to the gateway's international format. */
export function internationalPhone(value: string): string | null {
  const number = value.replace(/[\s().-]/g, '')
  return /^\+[1-9]\d{1,14}$/.test(number) ? number : null
}
