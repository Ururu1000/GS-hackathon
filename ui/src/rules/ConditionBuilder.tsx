// Visual editor for the typed condition tree: nested all/any groups, `not`,
// typed values, and each node's live test result from the test panel.

import { useEffect, useState } from 'react'
import { COND_OPS, type Condition, type CondOp, type Leaf, type NodeDiagnostic, type Scalar } from '../api/types'
import { ChevronDownIcon, ChevronUpIcon, CloseIcon } from '../lib/icons'
import {
  addChild,
  isGroup,
  isLeaf,
  kids,
  kindOf,
  LIST_OPS,
  moveChild,
  newLeaf,
  NO_VALUE,
  OP_LABEL,
  parseScalar,
  removeNode,
  retargetOp,
  setNode,
  showScalar,
  toggleNot,
  type ValueKind,
} from './draft'
import { PathInput, type EditorEnv } from './sections'

interface Props {
  value: Condition
  onChange: (c: Condition) => void
  env: EditorEnv
  diagnostics: Map<string, NodeDiagnostic>
}

const DIAG_TEXT: Record<string, string> = { match: 'Matches', no_match: 'No match', skipped: 'Not checked', error: 'Error' }

function Diag({ d }: { d: NodeDiagnostic | undefined }) {
  if (!d) return <span className="diag diag-none" aria-hidden="true" />
  const actual = 'actual' in d ? `Value in the event: ${JSON.stringify(d.actual)}` : null
  const title = [DIAG_TEXT[d.result], d.message, actual].filter(Boolean).join(' · ')
  return (
    <span className={`diag diag-${d.result}`} title={title} role="img" aria-label={title}>
      {d.result === 'match' ? '✓' : d.result === 'no_match' ? '✗' : d.result === 'error' ? '!' : '–'}
    </span>
  )
}

export function ConditionBuilder({ value, onChange, env, diagnostics }: Props) {
  // The root is always a group so there's somewhere to add conditions.
  const root: Condition = isGroup(value) ? value : { all: [value] }
  return <Node node="" c={root} root={root} onRoot={onChange} env={env} diagnostics={diagnostics} depth={0} />
}

interface NodeProps {
  node: string
  c: Condition
  root: Condition
  onRoot: (c: Condition) => void
  env: EditorEnv
  diagnostics: Map<string, NodeDiagnostic>
  depth: number
  negated?: boolean
  /** Address of the wrapping `not` node, when `negated`. */
  notNode?: string
  siblings?: number
}

function Node(props: NodeProps) {
  const { node, c } = props
  if ('not' in c) return <Node {...props} node={node === '' ? '0' : `${node}.0`} c={c.not} negated notNode={node} />
  if (isLeaf(c)) return <LeafRow {...props} leaf={c} />
  return <Group {...props} />
}

function NodeMenu({ node, root, onRoot, negated, notNode, siblings }: { node: string; root: Condition; onRoot: (c: Condition) => void; negated?: boolean; notNode?: string; siblings?: number }) {
  const self = negated && notNode !== undefined ? notNode : node
  const idx = Number(self.split('.').pop())
  if (self === '') return null
  return (
    <span className="node-menu">
      <button type="button" className={`btn btn-xs ${negated ? 'is-on' : ''}`} aria-pressed={!!negated} onClick={() => onRoot(toggleNot(root, self))} title="Match when this is NOT true">
        Not
      </button>
      <button type="button" className="icon-btn sm move-btn" disabled={idx === 0} onClick={() => onRoot(moveChild(root, self, -1))} aria-label="Move up" title="Move up"><ChevronUpIcon size={14} /></button>
      <button type="button" className="icon-btn sm move-btn" disabled={siblings !== undefined && idx >= siblings - 1} onClick={() => onRoot(moveChild(root, self, 1))} aria-label="Move down" title="Move down"><ChevronDownIcon size={14} /></button>
      <button type="button" className="icon-btn sm" onClick={() => onRoot(removeNode(root, self))} aria-label="Remove" title="Remove">
        <CloseIcon size={13} />
      </button>
    </span>
  )
}

function Group({ node, c, root, onRoot, env, diagnostics, depth, negated, notNode, siblings }: NodeProps) {
  const all = 'all' in c
  const list = kids(c)
  const d = diagnostics.get(negated && notNode !== undefined ? notNode : node)
  const setKind = (k: 'all' | 'any') => onRoot(setNode(root, node, k === 'all' ? { all: list } : { any: list }))
  const child = (i: number) => (node === '' ? String(i) : `${node}.${i}`)
  return (
    <div className={`cond-group depth-${Math.min(depth, 3)} ${negated ? 'is-negated' : ''}`}>
      <div className="cond-group-head">
        <Diag d={d} />
        {negated && <span className="not-tag">NOT</span>}
        <span className="small">Match</span>
        <select className="select select-sm" value={all ? 'all' : 'any'} onChange={(e) => setKind(e.target.value as 'all' | 'any')} aria-label="Group logic">
          <option value="all">all of these (AND)</option>
          <option value="any">any of these (OR)</option>
        </select>
        <span className="spacer" />
        <NodeMenu node={node} root={root} onRoot={onRoot} negated={negated} notNode={notNode} siblings={siblings} />
      </div>
      <div className="cond-children">
        {list.length === 0 && <p className="hint">No conditions yet: this group matches every event.</p>}
        {list.map((ch, i) => (
          <Node key={i} node={child(i)} c={ch} root={root} onRoot={onRoot} env={env} diagnostics={diagnostics} depth={depth + 1} siblings={list.length} />
        ))}
        <div className="inline">
          <button type="button" className="btn btn-sm" onClick={() => onRoot(addChild(root, node, newLeaf()))}>Add condition</button>
          {depth < 3 && (
            <button type="button" className="btn btn-sm" onClick={() => onRoot(addChild(root, node, { [all ? 'any' : 'all']: [newLeaf()] } as Condition))}>
              Add {all ? 'OR' : 'AND'} group
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

function LeafRow({ node, leaf, root, onRoot, env, diagnostics, negated, notNode, siblings }: NodeProps & { leaf: Leaf }) {
  const set = (l: Leaf) => onRoot(setNode(root, node, l))
  const d = diagnostics.get(negated && notNode !== undefined ? notNode : node)
  const inner = negated ? diagnostics.get(node) : undefined
  const sample = env.paths.find((p) => p.path === leaf.path)
  return (
    <div className={`cond-leaf ${negated ? 'is-negated' : ''}`}>
      <Diag d={d} />
      {negated && <span className="not-tag" title={inner ? `Inner condition: ${DIAG_TEXT[inner.result]}` : undefined}>NOT</span>}
      <PathInput value={leaf.path} onChange={(path) => set({ ...leaf, path })} env={env} label="Field" className="leaf-path" />
      <select className="select select-sm leaf-op" value={leaf.op} onChange={(e) => set(retargetOp(leaf, e.target.value as CondOp))} aria-label="Operator">
        {COND_OPS.map((op) => (
          <option key={op} value={op}>{OP_LABEL[op]}</option>
        ))}
      </select>
      {!NO_VALUE.includes(leaf.op) && <ValueInput leaf={leaf} onChange={(value) => set({ ...leaf, value })} sampleKind={sample ? kindOf(sample.value) : undefined} />}
      <NodeMenu node={node} root={root} onRoot={onRoot} negated={negated} notNode={notNode} siblings={siblings} />
      {d && d.result !== 'match' && d.result !== 'skipped' && (d.message || 'actual' in d) && (
        <div className="leaf-note small">
          {d.message ?? <>In this event: <code>{JSON.stringify(d.actual)}</code></>}
        </div>
      )}
    </div>
  )
}

/** Typed value: text, number or true/false; lists for "is one of". Local text
 * state so typing "1." or "a," isn't reformatted under the cursor. */
function ValueInput({ leaf, onChange, sampleKind }: { leaf: Leaf; onChange: (v: Scalar | Scalar[]) => void; sampleKind?: ValueKind }) {
  const list = LIST_OPS.includes(leaf.op)
  const values = Array.isArray(leaf.value) ? leaf.value : [leaf.value ?? '']
  const initialKind = kindOf(values.find((v) => v !== '') ?? (sampleKind === 'number' ? 0 : ''))
  const [kind, setKind] = useState<ValueKind>(initialKind)
  const fmt = () => (list ? values.map(showScalar).join(', ') : showScalar(values[0]))
  const [text, setText] = useState(fmt)

  // Follow outside changes (YAML tab, operator change) without fighting the cursor.
  const external = JSON.stringify(leaf.value)
  useEffect(() => {
    const parsed = list ? text.split(',').map((s) => parseScalar(s.trim(), kind)).filter((v) => v !== '') : parseScalar(text, kind)
    if (JSON.stringify(parsed) !== external) {
      setText(fmt())
      setKind(initialKind)
    }
  }, [external, list])

  const emit = (t: string, k: ValueKind) => {
    setText(t)
    onChange(list ? t.split(',').map((s) => parseScalar(s.trim(), k)).filter((v) => v !== '') : parseScalar(t, k))
  }
  const invalid = kind === 'number' && (list ? text.split(',').some((s) => s.trim() !== '' && !Number.isFinite(Number(s.trim().replace(/[_,\s]/g, '')))) : text.trim() !== '' && !Number.isFinite(Number(text.replace(/[_,\s]/g, ''))))

  return (
    <span className="leaf-value">
      {kind === 'bool' && !list ? (
        <select className="select select-sm" value={String(values[0] === true)} onChange={(e) => emit(e.target.value, 'bool')} aria-label="Value">
          <option value="true">true</option>
          <option value="false">false</option>
        </select>
      ) : (
        <input
          className="input input-sm mono"
          value={text}
          onChange={(e) => emit(e.target.value, kind)}
          aria-label={list ? 'Values, comma-separated' : 'Value'}
          aria-invalid={invalid || undefined}
          placeholder={list ? 'A, B, C' : 'value'}
          spellCheck={false}
        />
      )}
      <select
        className="select select-sm kind"
        value={kind}
        onChange={(e) => {
          const k = e.target.value as ValueKind
          setKind(k)
          emit(k === 'bool' && !list ? 'true' : text, k)
        }}
        aria-label="Value type"
        title="Value type"
      >
        <option value="text">abc</option>
        <option value="number">123</option>
        <option value="bool">yes/no</option>
      </select>
    </span>
  )
}
