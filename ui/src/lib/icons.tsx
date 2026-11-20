// UI icons on one 24-unit grid: 1.75 stroke, round caps and joins, currentColor.
// Severity icons are separate (alert/badges.tsx): solid shapes, glyph cut out.

interface IconProps {
  size?: number
  className?: string
}

function Svg({ size = 16, className, children }: IconProps & { children: React.ReactNode }) {
  return (
    <svg
      className={className}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.75"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  )
}

export const CloseIcon = (p: IconProps) => (
  <Svg {...p}>
    <path d="M6.5 6.5l11 11M17.5 6.5l-11 11" />
  </Svg>
)

export const CheckIcon = (p: IconProps) => (
  <Svg {...p}>
    <path d="M5 12.5l4.5 4.5L19 7.5" strokeWidth="2.5" />
  </Svg>
)

export const CopyIcon = (p: IconProps) => (
  <Svg {...p}>
    <rect x="8.5" y="8.5" width="11" height="11" rx="2.5" />
    <path d="M15.5 8.5V6.5a2 2 0 0 0-2-2h-7a2 2 0 0 0-2 2v7a2 2 0 0 0 2 2h2" />
  </Svg>
)

export const SearchIcon = (p: IconProps) => (
  <Svg {...p}>
    <circle cx="10.75" cy="10.75" r="6.25" />
    <path d="M15.5 15.5l4 4" />
  </Svg>
)

export const SunIcon = (p: IconProps) => (
  <Svg {...p}>
    <circle cx="12" cy="12" r="4" />
    <path d="M12 2.5v2M12 19.5v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2.5 12h2M19.5 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41" />
  </Svg>
)

export const MoonIcon = (p: IconProps) => (
  <Svg {...p}>
    <path d="M20.25 14.5A7.75 7.75 0 0 1 9.5 3.75a7.75 7.75 0 1 0 10.75 10.75Z" />
  </Svg>
)

export const KeyboardIcon = (p: IconProps) => (
  <Svg {...p}>
    <rect x="2.75" y="5.5" width="18.5" height="13" rx="2.5" />
    <path d="M6.75 9.5h.01M10.25 9.5h.01M13.75 9.5h.01M17.25 9.5h.01M6.75 13h.01M17.25 13h.01M9.5 14.5h5" />
  </Svg>
)

export const StatusIcon = ({ status, ...p }: IconProps & { status: 'open' | 'acked' | 'resolved' }) => (
  <Svg {...p}>
    {status === 'open' && (
      <>
        <circle cx="12" cy="12" r="8.25" />
        <circle cx="12" cy="12" r="2.5" fill="currentColor" stroke="none" />
      </>
    )}
    {status === 'acked' && (
      <>
        <path d="M2.5 12s3.5-6.5 9.5-6.5S21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12Z" />
        <circle cx="12" cy="12" r="2.75" />
      </>
    )}
    {status === 'resolved' && (
      <>
        <circle cx="12" cy="12" r="8.25" />
        <path d="m8.5 12.25 2.5 2.5 4.75-5" />
      </>
    )}
  </Svg>
)

/** Brand logo: a stencil "B" (stem + two bowls) throwing light. Black badge, white mark; inverts with the theme.
 * Same artwork as public/favicon.svg. */
export const BeaconLogo = ({ size = 26, className }: IconProps) => (
  <svg className={className} width={size} height={size} viewBox="0 0 32 32" fill="none" aria-hidden="true">
    <rect width="32" height="32" rx="8" fill="currentColor" />
    <g transform="translate(-2 0)">
      <g fill="var(--background)">
        <rect x="8" y="6.5" width="4.5" height="19" rx="2.25" />
        <path d="M15.5 6.5h2.2a4.6 4.6 0 0 1 0 9.2h-2.2Z" />
        <path d="M15.5 17.3h3a4.1 4.1 0 0 1 0 8.2h-3Z" />
      </g>
      <path d="M24.2 6.4l2.3-2.2M25.2 11h3" stroke="var(--background)" strokeWidth="1.8" strokeLinecap="round" />
    </g>
  </svg>
)
