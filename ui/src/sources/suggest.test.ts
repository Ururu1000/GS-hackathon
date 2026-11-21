import { expect, it } from 'vitest'
import { suggestionLabel } from './SourcesScreen'

it('labels who proposed a mapping', () => {
  expect(suggestionLabel({ provider: 'mock', model: null })).toBe('Mock suggestion')
  expect(suggestionLabel({ provider: 'claude', model: 'claude-opus-5-5' })).toBe('Suggested by Claude (claude-opus-5-5)')
  expect(suggestionLabel({ provider: 'claude', model: null })).toBe('Suggested by Claude')
})
